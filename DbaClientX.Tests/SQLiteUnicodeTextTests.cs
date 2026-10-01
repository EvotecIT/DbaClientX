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
