using DBAClientX.QueryBuilder;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SqlIdentifierTests
{
    /// <summary>Names that break naive quoting: every dialect's quote characters, statement separators and comments.</summary>
    public static readonly string[] HostileNames =
    {
        "plain",
        "with space",
        "a\"b",
        "a]b",
        "a[b",
        "a`b",
        "a'b",
        "x\"; DROP TABLE t; --",
        "x]; DROP TABLE t; --",
        "x`; DROP TABLE t; --",
        "dotted.name",
        "/* comment */",
        "Zażółć 日本語 😀",
        " leading and trailing "
    };

    [Theory]
    [InlineData(SqlDialect.SqlServer, "a]b", "[a]]b]")]
    [InlineData(SqlDialect.SqlServer, "x]; DROP TABLE t; --", "[x]]; DROP TABLE t; --]")]
    [InlineData(SqlDialect.SqlServer, "a\"b`c", "[a\"b`c]")]
    [InlineData(SqlDialect.MySql, "a`b", "`a``b`")]
    [InlineData(SqlDialect.MySql, "a\"b]c", "`a\"b]c`")]
    [InlineData(SqlDialect.PostgreSql, "a\"b", "\"a\"\"b\"")]
    [InlineData(SqlDialect.SQLite, "x\"; DROP TABLE t; --", "\"x\"\"; DROP TABLE t; --\"")]
    [InlineData(SqlDialect.Oracle, "a]b`c", "\"a]b`c\"")]
    [InlineData(SqlDialect.PostgreSql, "dotted.name", "\"dotted.name\"")]
    public void Quote_DoublesTheClosingQuoteOfTheDialect(SqlDialect dialect, string identifier, string expected)
        => Assert.Equal(expected, SqlIdentifier.Quote(dialect, identifier));

    [Theory]
    [InlineData(SqlDialect.SqlServer, null)]
    [InlineData(SqlDialect.SqlServer, "")]
    [InlineData(SqlDialect.SQLite, "a\0b")]
    [InlineData(SqlDialect.MySql, "\0")]
    [InlineData(SqlDialect.Oracle, "a\"b")]
    public void Quote_RejectsNamesNoQuotedIdentifierCanHold(SqlDialect dialect, string? identifier)
        => Assert.Throws<ArgumentException>(() => SqlIdentifier.Quote(dialect, identifier!));

    public static TheoryData<SqlDialect, string> HostileNamesPerDialect()
    {
        var data = new TheoryData<SqlDialect, string>();
        foreach (var dialect in Enum.GetValues<SqlDialect>())
        {
            foreach (var name in HostileNames.Concat(new[] { " ", "＂］｀ look-alikes", new string('n', 300) }))
            {
                if (dialect != SqlDialect.Oracle || !name.Contains('"'))
                {
                    data.Add(dialect, name);
                }
            }
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(HostileNamesPerDialect))]
    public void Quote_EveryHostileNameStaysOneQuotedIdentifier(SqlDialect dialect, string name)
    {
        var quoted = SqlIdentifier.Quote(dialect, name);
        var (open, close) = dialect switch
        {
            SqlDialect.SqlServer => ('[', ']'),
            SqlDialect.MySql => ('`', '`'),
            _ => ('"', '"')
        };

        Assert.Equal(open, quoted[0]);
        Assert.Equal(close, quoted[^1]);
        // Inside, the closing character only appears doubled, so the parser never sees the identifier end early.
        var inner = quoted.Substring(1, quoted.Length - 2);
        for (var i = 0; i < inner.Length; i++)
        {
            if (inner[i] == close)
            {
                Assert.True(i + 1 < inner.Length && inner[i + 1] == close, $"Lone closing quote at {i} in {quoted}.");
                i++;
            }
        }

        Assert.Equal(name, inner.Replace(new string(close, 2), close.ToString()));
    }

    [Fact]
    public void Quote_RejectsAnUndefinedDialect()
        => Assert.Throws<ArgumentOutOfRangeException>(() => SqlIdentifier.Quote((SqlDialect)42, "name"));

    [Fact]
    public void Compiler_QuotesEachDottedPartWithTheSharedRules()
    {
        var sql = new Query().Select("t.a]b", "t.*").From("dbo.x]y", "t").Compile(SqlDialect.SqlServer);

        Assert.Equal("SELECT [t].[a]]b], [t].* FROM [dbo].[x]]y] AS [t]", sql);
        Assert.Throws<ArgumentException>(() => new Query().Select("a\0b").From("t").Compile(SqlDialect.SQLite));
        var empty = Assert.Throws<ArgumentException>(() => new Query().Select("a..b").From("t").Compile(SqlDialect.PostgreSql));
        Assert.Contains("a..b", empty.Message);
        Assert.Throws<ArgumentException>(() => new Query().Select("a\"b").From("t").Compile(SqlDialect.Oracle));
    }

    [Fact]
    public void Quote_HostileNamesRoundTripThroughSqlite()
    {
        var path = Path.Combine(Path.GetTempPath(), "dbx-ident-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var connection = new SqliteConnection("Data Source=" + path + ";Pooling=False");
            connection.Open();
            var table = SqlIdentifier.Quote(SqlDialect.SQLite, "t\"; DROP TABLE x; --");
            var columns = HostileNames.Select(name => SqlIdentifier.Quote(SqlDialect.SQLite, name)).ToArray();
            Execute(connection, "CREATE TABLE " + table + " (" + string.Join(", ", columns.Select(c => c + " TEXT")) + ")");
            Execute(connection, "INSERT INTO " + table + " VALUES (" + string.Join(", ", HostileNames.Select((_, i) => "'v" + i + "'")) + ")");

            using var command = connection.CreateCommand();
            command.CommandText = "SELECT " + string.Join(", ", columns) + " FROM " + table;
            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            for (var i = 0; i < HostileNames.Length; i++)
            {
                Assert.Equal(HostileNames[i], reader.GetName(i));
                Assert.Equal("v" + i, reader.GetString(i));
            }
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            File.Delete(path);
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public void Quote_HostileNamesRoundTripThroughSqlServer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        using var connection = new SqlConnection(connectionString);
        connection.Open();
        var table = SqlIdentifier.Quote(SqlDialect.SqlServer, "#t]; DROP TABLE x; --");
        // SQL Server trims trailing spaces from identifiers, so that name is left out.
        var names = HostileNames.Where(name => !name.EndsWith(' ')).ToArray();
        var columns = names.Select(name => SqlIdentifier.Quote(SqlDialect.SqlServer, name)).ToArray();
        using (var create = new SqlCommand("CREATE TABLE " + table + " (" + string.Join(", ", columns.Select(c => c + " nvarchar(10)")) + "); INSERT INTO " + table + " VALUES (" + string.Join(", ", names.Select((_, i) => "N'v" + i + "'")) + ")", connection))
        {
            create.ExecuteNonQuery();
        }

        using var select = new SqlCommand("SELECT " + string.Join(", ", columns) + " FROM " + table, connection);
        using var reader = select.ExecuteReader();
        Assert.True(reader.Read());
        for (var i = 0; i < names.Length; i++)
        {
            Assert.Equal(names[i], reader.GetName(i));
            Assert.Equal("v" + i, reader.GetString(i));
        }
    }

    private static void Execute(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
