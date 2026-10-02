using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;

namespace DbaClientX.Tests;

public sealed class SqlStatementTextTests
{
    [Fact]
    public void Split_ReturnsEachStatementWithoutSeparatorsOrSurroundingComments()
    {
        const string sql =
            "-- header\n" +
            "CREATE TEMP TABLE s (Id INTEGER, Name TEXT); ;\n" +
            "INSERT INTO s VALUES (1, 'a;b'), (2, \"x;\" || [c;d] || `e;f`) /* inner; comment */;\n" +
            "/* between; */ SELECT COUNT(*) FROM s -- trailing; comment\n";

        var statements = SqlStatementText.Split(sql);

        Assert.Equal(new[]
        {
            "CREATE TEMP TABLE s (Id INTEGER, Name TEXT)",
            "INSERT INTO s VALUES (1, 'a;b'), (2, \"x;\" || [c;d] || `e;f`)",
            "SELECT COUNT(*) FROM s"
        }, statements);
        Assert.False(SqlStatementText.IsSingleStatement(sql));
    }

    [Fact]
    public void Split_KeepsSqliteTriggerBodiesWhole()
    {
        const string trigger =
            "CREATE TEMPORARY TRIGGER t AFTER INSERT ON s WHEN CASE WHEN NEW.Id > 0 THEN 1 END BEGIN " +
            "UPDATE s SET Name = CASE WHEN NEW.end = 1 THEN 'one' ELSE 'many' END, end = NEW.end WHERE Id = NEW.Id; " +
            "DELETE FROM s WHERE Id < 0; END";
        const string sql = trigger + ";\nBEGIN; UPDATE s SET Id = 1; END;";

        var statements = SqlStatementText.Split(sql);

        Assert.Equal(new[] { trigger, "BEGIN", "UPDATE s SET Id = 1", "END" }, statements);
        Assert.True(SqlStatementText.IsSingleStatement(trigger + ";"));
    }

    [Fact]
    public void Split_ReadsDollarQuotesAsStringsInPostgreSqlOnly()
    {
        const string function = "CREATE FUNCTION f() RETURNS int AS $body$ SELECT 1; SELECT 2; $body$ LANGUAGE sql";
        const string sql = function + "; SELECT $1, $$a;b$$";

        Assert.Equal(new[] { function, "SELECT $1, $$a;b$$" }, SqlStatementText.Split(sql, SqlDialect.PostgreSql));

        // In SQLite $a$ and $$ are parameter names: a statement between two of them is still a statement.
        Assert.Equal(new[] { "SELECT $a$", "DELETE FROM t", "SELECT $a$" }, SqlStatementText.Split("SELECT $a$; DELETE FROM t; SELECT $a$"));
        Assert.False(SqlStatementText.IsSingleStatement("SELECT $$; DELETE FROM t"));
    }

    [Theory]
    [InlineData("SELECT 'a\\';b'; SELECT 2;", "SELECT 'a\\';b'")]
    [InlineData("SELECT \"a\\\";b\"; SELECT 2;", "SELECT \"a\\\";b\"")]
    [InlineData("SELECT N'a\\';b'; SELECT 2;", "SELECT N'a\\';b'")]
    public void Split_MySqlBackslashEscapes_KeepsSemicolonsInsideStrings(string sql, string first)
    {
        Assert.Equal(new[] { first, "SELECT 2" }, SqlStatementText.Split(sql, SqlDialect.MySql));
    }

    [Theory]
    [InlineData("", 0)]
    [InlineData(" ;; -- nothing\n/* here */ ", 0)]
    [InlineData("SELECT 1", 1)]
    [InlineData(";SELECT ';' AS semicolon; -- trailing comment", 1)]
    [InlineData("BEGIN; UPDATE s SET Id = 1; COMMIT;", 3)]
    public void Split_CountsStatements(string sql, int count)
    {
        Assert.Equal(count, SqlStatementText.Split(sql).Count);
        Assert.Equal(count <= 1, SqlStatementText.IsSingleStatement(sql));
    }
}
