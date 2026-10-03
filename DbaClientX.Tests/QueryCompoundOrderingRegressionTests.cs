using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public class QueryCompoundOrderingRegressionTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void CompoundQualifiedOrdering_UsesTheProjectedName(bool aliased, bool mixed)
    {
        var query = Operand(aliased).Union(Operand(aliased));
        if (mixed) query.Intersect(Operand(aliased));
        query.OrderByDescending("n.Id").Limit(1);
        string sql = query.Compile(SqlDialect.SqlServer);
        Assert.EndsWith(aliased ? "ORDER BY [Output.Id] DESC" : "ORDER BY [Id] DESC", sql);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServerCompoundQualifiedOrdering_ReturnsTheRequestedRow(bool aliased)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server test database.");
        using var client = new DBAClientX.SqlServer();
        foreach (bool mixed in new[] { false, true })
        {
            var query = Operand(aliased).Union(Operand(aliased));
            if (mixed) query.Intersect(Operand(aliased));
            query.OrderByDescending("n.Id").Limit(1);
            var values = await client.QueryAsListAsync(connection!, query.Compile(), row => row.GetInt32(0));
            Assert.Equal(new[] { 2 }, values);
        }
    }

    [Theory]
    [InlineData("Name LIKE '%!_%' ESCAPE '!'")]
    [InlineData("Name LIKE '%!_%' escape '!'")]
    public void EscapeLiteral_IsAnExpressionTail(string expression)
    {
        Query Operand() => new Query().SelectRaw(expression + ", Code LIKE '%!_%' ESCAPE '!'")
            .FromRaw("(SELECT 'a_b' AS Name, 'c_d' AS Code) AS n");
        string sql = Operand().Union(Operand()).Intersect(Operand()).Compile(SqlDialect.MySql);
        Assert.Contains("SELECT `dbx_column_0`, `dbx_column_1`", sql);
        Assert.DoesNotContain("AS `!`", sql);
    }

    [Theory]
    [InlineData("n.ESCAPE 'OutputId'")]
    [InlineData("n.ESCAPE AS OutputId")]
    public void QualifiedOperatorName_RetainsItsRealAlias(string expression)
    {
        Query Operand() => new Query().SelectRaw(expression + ", 2 AS divisor")
            .FromRaw("(SELECT 7 AS `ESCAPE`) AS n");
        string sql = Operand().Union(Operand()).Intersect(Operand()).Compile(SqlDialect.MySql);
        Assert.DoesNotContain("WITH ", sql);
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer)]
    [InlineData(SqlDialect.SQLite)]
    [InlineData(SqlDialect.MySql)]
    [InlineData(SqlDialect.PostgreSql)]
    [InlineData(SqlDialect.Oracle)]
    public void CompoundQualifiedOrdering_UsesAnOutputColumnAcrossDialects(SqlDialect dialect)
    {
        var query = Operand(false).Union(Operand(false)).Intersect(Operand(false)).OrderBy("n.Id").Limit(1);
        Assert.Contains("ORDER BY " + SqlIdentifier.Quote(dialect, "Id"), query.Compile(dialect));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServerDuplicateOutputNames_OrderByTheSelectedSourceColumn()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server test database.");
        Query Operand() => new Query().SelectRaw("n.Id, m.Id")
            .FromRaw("(SELECT 2 AS Id UNION ALL SELECT 1 AS Id) AS n CROSS JOIN (SELECT 3 AS Id) AS m");
        string sql = Operand().Union(Operand()).OrderBy("n.Id").Limit(1).Compile();
        using var client = new DBAClientX.SqlServer();
        var row = Assert.Single(await client.QueryAsListAsync(connection!, sql,
            record => (record.GetInt32(0), record.GetInt32(1))));
        Assert.Equal((1, 3), row);
    }

    private static Query Operand(bool aliased)
        => (aliased ? new Query().SelectRaw("n.Id AS [Output.Id]") : new Query().Select("n.Id"))
            .FromRaw("(SELECT 1 AS Id UNION ALL SELECT 2 AS Id) AS n");
}
