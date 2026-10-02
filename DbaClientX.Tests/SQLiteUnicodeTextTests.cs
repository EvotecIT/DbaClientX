using DBAClientX;
using DBAClientX.QueryBuilder;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SQLiteUnicodeTextTests : IDisposable
{
    private static readonly string[] Names = { "zażółć", "ZAŻÓŁĆ", "Émile", "émile", "apple", "Äpfel", "ápple", "Ωmega", "ωmega", "İstanbul", "istanbul", "𐐀deseret", "𐐨deseret", "B", "a" };

    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-unicode-" + Guid.NewGuid().ToString("N") + ".db");

    public SQLiteUnicodeTextTests()
    {
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(_database, "CREATE TABLE t (Id INTEGER PRIMARY KEY, Name TEXT)");
        for (var i = 0; i < Names.Length; i++)
        {
            sqlite.ExecuteNonQuery(_database, "INSERT INTO t VALUES (@id, @name)", new Dictionary<string, object?> { ["@id"] = i, ["@name"] = Names[i] });
        }
    }

    [Fact]
    public void Compare_OrdersLikeInvariantLowerCaseOrdinal()
    {
        var random = new Random(1234);
        var alphabet = "aAzZżŻóÓéÉßẞıIiİωΩ_%! 1".ToCharArray().Select(c => c.ToString()).Concat(new[] { "𐐀", "𐐨", "😀", "\uD800", "\uDC00", "Σ", "ς", "σ" }).ToArray();
        for (var round = 0; round < 20000; round++)
        {
            var left = string.Concat(Enumerable.Range(0, random.Next(0, 6)).Select(_ => alphabet[random.Next(alphabet.Length)]));
            var right = string.Concat(Enumerable.Range(0, random.Next(0, 6)).Select(_ => alphabet[random.Next(alphabet.Length)]));
            var expected = Math.Sign(string.CompareOrdinal(left.ToLowerInvariant(), right.ToLowerInvariant()));

            Assert.True(expected == Math.Sign(SQLiteUnicodeText.Compare(left, right)), $"'{left}' vs '{right}'");
        }
    }

    [Fact]
    public void NoCaseCollation_OrdersStoredUtf8LikeInvariantLowerCaseOrdinal()
    {
        // The collation compares the stored UTF-8 bytes, so the order of what is read back must equal the .NET order.
        var random = new Random(4321);
        var alphabet = "aAzZżŻóÓéÉßẞıIiİωΩ_%! 1kKKİ".ToCharArray().Select(c => c.ToString()).Concat(new[] { "𐐀", "𐐨", "😀", "Σ", "ς", "σ", "�" }).ToArray();
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        SQLiteUnicodeText.Register(connection);
        Execute(connection, "CREATE TABLE s (Id INTEGER PRIMARY KEY, Name TEXT)");
        using (var transaction = connection.BeginTransaction())
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO s (Name) VALUES ($name)";
            var name = insert.Parameters.Add("$name", SqliteType.Text);
            for (var row = 0; row < 3000; row++)
            {
                // Mostly short texts that share prefixes, some past the 128-character stack buffers.
                var length = row % 50 == 0 ? random.Next(120, 400) : random.Next(0, 7);
                name.Value = string.Concat(Enumerable.Range(0, length).Select(_ => alphabet[random.Next(alphabet.Length)]));
                insert.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        // Invalid UTF-8 decodes to U+FFFD, as the provider reads it.
        Execute(connection, "INSERT INTO s (Name) VALUES (CAST(X'61C3' AS TEXT)), (CAST(X'61FF62' AS TEXT)), (CAST(X'41EFBFBD' AS TEXT)), (CAST(X'61E08062' AS TEXT)), (CAST(X'F0908041' AS TEXT)), (CAST(X'EDA080C5BB' AS TEXT)), (CAST(X'C5BBF0' AS TEXT))");

        var rows = new List<(long Id, string Name)>();
        using (var select = connection.CreateCommand())
        {
            select.CommandText = "SELECT Id, Name FROM s ORDER BY Name COLLATE DBX_NOCASE, Id";
            using var reader = select.ExecuteReader();
            while (reader.Read())
            {
                rows.Add((reader.GetInt64(0), reader.GetString(1)));
            }
        }

        var expected = rows
            .OrderBy(row => row.Name, Comparer<string>.Create((a, b) => string.CompareOrdinal(a.ToLowerInvariant(), b.ToLowerInvariant())))
            .ThenBy(row => row.Id)
            .Select(row => row.Id);
        Assert.Equal(expected, rows.Select(row => row.Id));
    }

    [Fact]
    public void Register_WhileAStatementRuns_ThrowsAndKeepsTheConnectionUsable()
    {
        using var connection = new SqliteConnection("Data Source=:memory:");
        connection.Open();
        SQLiteUnicodeText.Register(connection);
        SQLiteUnicodeText.Register(connection);
        using (var command = connection.CreateCommand())
        {
            command.CommandText = "SELECT 1 UNION ALL SELECT 2";
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());

            // SQLite refuses to replace a collation while a statement runs; registering must fail before anything is replaced.
            Assert.Throws<InvalidOperationException>(() => SQLiteUnicodeText.Register(connection));
        }

        // Once the statement is done, registering works again (it replaces the probe the refusal left behind).
        SQLiteUnicodeText.Register(connection);
        using var sort = connection.CreateCommand();
        sort.CommandText = "SELECT 'B' < 'a' COLLATE DBX_NOCASE, dbx_lower('Ż')";
        using var result = sort.ExecuteReader();
        Assert.True(result.Read());
        Assert.Equal(0L, result.GetInt64(0));
        Assert.Equal("ż", result.GetString(1));
        using var closed = new SqliteConnection("Data Source=:memory:");
        Assert.Throws<InvalidOperationException>(() => SQLiteUnicodeText.Register(closed));
    }

    [Fact]
    public async Task ConfigureConnection_RegistersUnicodeFoldingForQueriesSortsAndContains()
    {
        using var sqlite = new SQLite { ConfigureConnection = connection => SQLiteUnicodeText.Register(connection) };

        var sorted = await sqlite.QueryReadOnlyAsListAsync(_database, "SELECT Name FROM t ORDER BY Name COLLATE DBX_NOCASE, Id", reader => reader.GetString(0));
        var equal = await sqlite.QueryReadOnlyAsListAsync(_database, "SELECT Id FROM t WHERE dbx_lower(Name) = dbx_lower(@p0) ORDER BY Id", reader => reader.GetInt64(0), new Dictionary<string, object?> { ["@p0"] = "ZAŻÓŁĆ" });
        var (sql, parameters) = new Query().Select("Id").From("t").WhereContainsRaw("dbx_lower(\"Name\")", "ŻÓ".ToLowerInvariant()).OrderBy("Id").CompileWithNamedParameters(SqlDialect.SQLite);
        var contains = await sqlite.QueryReadOnlyAsListAsync(_database, sql, reader => reader.GetInt64(0), parameters);

        var expectedOrder = Names.Select((name, id) => (name, id))
            .OrderBy(x => x.name, Comparer<string>.Create((a, b) => string.CompareOrdinal(a.ToLowerInvariant(), b.ToLowerInvariant())))
            .ThenBy(x => x.id)
            .Select(x => x.name);
        Assert.Equal(expectedOrder, sorted);
        Assert.Equal(new long[] { 0, 1 }, equal);
        Assert.Equal(new long[] { 0, 1 }, contains);
    }

    [Fact]
    public async Task Register_OnAPooledConnection_LeavesTheNextUserWithTheBuiltIns()
    {
        // Pooled connection-string APIs reuse native handles: registrations must not outlive the connection.
        var connectionString = "Data Source=" + _database + ";Pooling=True";
        using var unicode = new SQLite { ConfigureConnection = connection => SQLiteUnicodeText.Register(connection), ReturnType = ReturnType.DataTable };
        using var plain = new SQLite { ReturnType = ReturnType.DataTable };

        var folded = Assert.IsType<System.Data.DataTable>(await unicode.QueryWithConnectionStringAsync(connectionString, "SELECT dbx_lower('Ż'), lower('Ż')"));
        var builtIn = Assert.IsType<System.Data.DataTable>(await plain.QueryWithConnectionStringAsync(connectionString, "SELECT lower('Ż'), lower('A')"));

        Assert.Equal("ż", folded.Rows[0][0]);
        Assert.Equal("Ż", folded.Rows[0][1]);
        Assert.Equal("Ż", builtIn.Rows[0][0]);
        Assert.Equal("a", builtIn.Rows[0][1]);
        await Assert.ThrowsAsync<DbaQueryExecutionException>(() => plain.QueryWithConnectionStringAsync(connectionString, "SELECT dbx_lower('Ż')"));
    }
    [Fact]
    public async Task ConfigureConnection_RunsForEveryConnectionTheClientOpens()
    {
        var configured = 0;
        using var sqlite = new SQLite
        {
            ConfigureConnection = connection =>
            {
                Interlocked.Increment(ref configured);
                SQLiteUnicodeText.Register(connection);
            }
        };
        const string probe = "SELECT dbx_lower('Ż')";

        Assert.Equal("ż", sqlite.ExecuteScalar(_database, probe));
        Assert.Equal("ż", await sqlite.ExecuteScalarAsync(_database, probe));
        Assert.Equal(new[] { "ż" }, await sqlite.QueryReadOnlyAsListAsync(_database, probe, reader => reader.GetString(0)));
        await foreach (var value in sqlite.QueryReadOnlyStreamAsync(_database, probe, record => record.GetString(0)))
        {
            Assert.Equal("ż", value);
        }

        sqlite.BeginTransaction(_database);
        Assert.Equal("ż", sqlite.ExecuteScalar(_database, probe, useTransaction: true));
        sqlite.Commit();
        using (var session = sqlite.OpenSession(_database))
        {
            Assert.Equal("ż", session.ExecuteScalar(probe));
        }

        using (var connection = sqlite.OpenDbConnection(_database))
        using (var command = connection.CreateCommand())
        {
            command.CommandText = probe;
            Assert.Equal("ż", command.ExecuteScalar());
        }

        Assert.True((await sqlite.CheckIntegrityAsync(_database)).IsHealthy);
        Assert.Equal(8, configured);
    }

    [Fact]
    public async Task ConfigureConnection_WhenCallbackThrows_FailsTheOperation()
    {
        using var sqlite = new SQLite { ConfigureConnection = _ => throw new InvalidOperationException("configuration failed") };

        var exception = await Assert.ThrowsAsync<DbaQueryExecutionException>(() => sqlite.QueryReadOnlyAsListAsync(_database, "SELECT 1", reader => reader.GetInt64(0)));

        Assert.Equal(typeof(InvalidOperationException).FullName, exception.ProviderExceptionType);
    }

    private static void Execute(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_database);
        }
        catch (IOException)
        {
        }
    }
}
