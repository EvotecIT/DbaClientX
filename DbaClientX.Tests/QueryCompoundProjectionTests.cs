using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QueryCompoundProjectionTests
{
    [Theory]
    [InlineData("Id", "Id")]
    [InlineData("numbers.Id", "Id")]
    [InlineData("[Id]", "Id")]
    [InlineData("Id OutputId", "OutputId")]
    [InlineData("Id 'OutputId'", "OutputId")]
    [InlineData("Id AS [Output.Id]", "Output.Id")]
    [InlineData("OutputId = Id", "OutputId")]
    [InlineData("Id + 1 OutputId", "OutputId")]
    [InlineData("CASE WHEN Id > 0 THEN Id ELSE 0 END OutputId", "OutputId")]
    [InlineData("CAST(Id AS int) OutputId", "OutputId")]
    [InlineData("Title COLLATE Latin1_General_100_BIN2 AS Title", "Title")]
    [InlineData("Title COLLATE Latin1_General_100_BIN2 OutputId", "OutputId")]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServerGeneratedWrapper_PreservesNamedExpressionMetadata(string expression, string name)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        Query Operand() => new Query().SelectRaw(expression).FromRaw("(SELECT 1 AS Id, 'sample' AS Title) AS numbers");
        using var client = new DBAClientX.SqlServer();
        var baseline = await client.QueryAsListAsync(connection!, Operand().Compile(), row => (row.GetName(0), row.GetValue(0)));
        var wrapped = await client.QueryAsListAsync(connection!, Operand().Union(Operand()).Limit(1).Compile(), row => (row.GetName(0), row.GetValue(0)));
        Assert.Equal(name, Assert.Single(baseline).Item1);
        Assert.Equal(Assert.Single(baseline), Assert.Single(wrapped));
    }

    [Theory]
    [InlineData("NULL")]
    [InlineData("CURRENT_TIMESTAMP")]
    [InlineData("CURRENT_USER")]
    [InlineData("CASE WHEN 1 > 0 THEN 1 ELSE 0 END")]
    [InlineData("CAST('sample' AS varchar(10)) COLLATE Latin1_General_100_BIN2")]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServerGeneratedWrapper_AcceptsUnnamedKeywordsAndExpressions(string expression)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        using var client = new DBAClientX.SqlServer();
        var query = new Query().SelectRaw(expression).Union(new Query().SelectRaw(expression)).Limit(1);
        var rows = await client.QueryAsListAsync(connection!, query.Compile(), row => (row.GetName(0), row.GetValue(0)));
        Assert.Equal("dbx_column_0", Assert.Single(rows).Item1);
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer)]
    [InlineData(SqlDialect.MySql)]
    public void DuplicateExplicitProjections_ReceiveUniqueInternalNamesAndKeepResultNames(SqlDialect dialect)
    {
        var query = Build("nested");
        string sql = query.Compile(dialect);
        Assert.Contains(SqlIdentifier.Quote(dialect, "dbx_column_0") + " AS " + SqlIdentifier.Quote(dialect, "Id"), sql);
        Assert.Contains("(" + SqlIdentifier.Quote(dialect, "dbx_column_0") + ", " + SqlIdentifier.Quote(dialect, "dbx_column_1") + ")", sql);
    }

    [Fact]
    public void SqlServerUnnamedCompoundLimit_AssignsDerivedColumnNames()
    {
        string sql = Build("literal").Compile(SqlDialect.SqlServer);
        Assert.Equal("SELECT TOP 1 [dbx_column_0] FROM (SELECT 1 UNION SELECT 2) AS [dbx_compound] ([dbx_column_0]) ORDER BY 1", sql);
    }

    [Theory]
    [InlineData("literal")]
    [InlineData("aggregate")]
    [InlineData("duplicate")]
    [InlineData("nested")]
    [InlineData("mixed")]
    [InlineData("null")]
    [InlineData("implicit")]
    [InlineData("assignment")]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServerCompiledCompound_PreservesValuesAndProjectionShape(string shape)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        using var client = new DBAClientX.SqlServer();
        var rows = await client.QueryAsListAsync(connection!, Build(shape).Compile(SqlDialect.SqlServer),
            row => new ProjectionRow(Enumerable.Range(0, row.FieldCount).Select(row.GetName).ToArray(),
                Enumerable.Range(0, row.FieldCount).Select(index => row.IsDBNull(index) ? (long?)null : Convert.ToInt64(row.GetValue(index))).ToArray()));
        AssertRows(shape, rows);
    }

    [Theory]
    [InlineData("literal")]
    [InlineData("aggregate")]
    [InlineData("duplicate")]
    [InlineData("nested")]
    [InlineData("mixed")]
    [InlineData("null")]
    [InlineData("implicit")]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlCompiledCompound_PreservesValuesAndProjectionShape(string shape)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL database.");
        using var client = new DBAClientX.MySql();
        var rows = await client.QueryAsListAsync(connection!, Build(shape).Compile(SqlDialect.MySql),
            row => new ProjectionRow(Enumerable.Range(0, row.FieldCount).Select(row.GetName).ToArray(),
                Enumerable.Range(0, row.FieldCount).Select(index => row.IsDBNull(index) ? (long?)null : Convert.ToInt64(row.GetValue(index))).ToArray()));
        AssertRows(shape, rows);
    }

    private static Query Build(string shape) => shape switch
    {
        "literal" => new Query().Select("1").Union(new Query().Select("2")).Limit(1).OrderByRaw("1"),
        "aggregate" => new Query().SelectRaw("COUNT(*)").FromRaw("(SELECT 1 AS Id) AS numbers")
            .Union(new Query().Select("2")).Limit(1).OrderByRaw("1"),
        "duplicate" => new Query().SelectRaw("1 AS Id, 2 AS Id")
            .UnionAll(new Query().SelectRaw("3 AS Id, 4 AS Id")).Limit(1).OrderByRaw("1"),
        "nested" => new Query().SelectRaw("1 AS Id, 2 AS Id")
            .UnionAll(new Query().SelectRaw("3 AS Id, 4 AS Id").Limit(1)).OrderByRaw("1"),
        "mixed" => new Query().Select("1").Union(new Query().Select("2"))
            .Intersect(new Query().Select("2")),
        "null" => new Query().SelectRaw("NULL").Union(new Query().SelectRaw("NULL")).Limit(1),
        "implicit" => new Query().SelectRaw("Id OutputId").FromRaw("(SELECT 1 AS Id) AS numbers")
            .Union(new Query().Select("2")).Limit(1).OrderBy("OutputId"),
        "assignment" => new Query().SelectRaw("OutputId = Id").FromRaw("(SELECT 1 AS Id) AS numbers")
            .Union(new Query().Select("2")).Limit(1).OrderBy("OutputId"),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    private static void AssertRows(string shape, IReadOnlyList<ProjectionRow> rows)
    {
        if (shape is "duplicate" or "nested")
        {
            Assert.Equal(shape == "nested" ? 2 : 1, rows.Count);
            Assert.Equal(new[] { "Id", "Id" }, rows[0].Names);
            Assert.Equal(new long?[] { 1, 2 }, rows[0].Values);
            if (shape == "nested") Assert.Equal(new long?[] { 3, 4 }, rows[1].Values);
        }
        else
        {
            var row = Assert.Single(rows);
            if (shape == "null") Assert.Null(Assert.Single(row.Values));
            else Assert.Equal(shape == "mixed" ? 2L : 1L, Assert.Single(row.Values));
            if (shape is "implicit" or "assignment") Assert.Equal("OutputId", Assert.Single(row.Names));
        }
    }

    private sealed record ProjectionRow(string[] Names, long?[] Values);
}
