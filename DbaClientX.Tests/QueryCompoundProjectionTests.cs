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
    [InlineData("Date OutputId", "OutputId")]
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
        Query Operand() => new Query().SelectRaw(expression).FromRaw("(SELECT 1 AS Id, 1 AS Date, 'sample' AS Title) AS numbers");
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
    [InlineData("CAST('2026-10-03T12:00:00' AS datetime2) AT TIME ZONE 'UTC'")]
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

    [Fact]
    public void MySqlScopedProjectionNames_PreserveSourceReferencesAndParameterOrder()
    {
        Query Build(int first, int second) => new Query().SelectRaw("Id AS Value, Id AS Value")
            .From("dbx_left_1_source").Where("Id", first)
            .Union(new Query().SelectRaw("Id AS Value, Id AS Value").From("other").Where("Id", second))
            .Intersect(new Query().SelectRaw("3, 3"));
        var first = Build(1, 2).CompileWithParameters(SqlDialect.MySql);
        var second = Build(4, 5).CompileWithParameters(SqlDialect.MySql);
        Assert.Contains("WITH `dbx_left_1_source_1` (`dbx_column_0`, `dbx_column_1`)", first.Sql);
        Assert.Contains("FROM `dbx_left_1_source` WHERE `Id` = @p0", first.Sql);
        Assert.Contains("FROM `other` WHERE `Id` = @p1", first.Sql);
        Assert.Equal(new object[] { 1, 2 }, first.Parameters);
        Assert.Equal(first.Sql, second.Sql);
        Assert.Equal(new object[] { 4, 5 }, second.Parameters);
    }

    [Theory]
    [InlineData("8 DIV divisor")]
    [InlineData("8 MOD divisor")]
    [InlineData("BINARY Name")]
    [InlineData("Created + INTERVAL 1 DAY")]
    [InlineData("Created + INTERVAL (divisor + 1) HOUR")]
    [InlineData("Name REGEXP _utf8mb4'x'")]
    [InlineData("Name RLIKE _utf8mb4'x'")]
    [InlineData("DATE '2026-10-03'")]
    [InlineData("X'78'")]
    [InlineData("B'01'")]
    public void MySqlExpressionTails_AreNotInventedAliases(string expression)
    {
        string sql = BuildExpressionTail(expression).Compile(SqlDialect.MySql);
        Assert.Contains("SELECT `dbx_column_0`, `dbx_column_1` AS `divisor`", sql);
    }

    [Theory]
    [InlineData("8 DIV divisor OutputValue")]
    [InlineData("Name REGEXP _utf8mb4'x' OutputValue")]
    [InlineData("Created + INTERVAL 1 DAY OutputValue")]
    [InlineData("Created + INTERVAL 1 DAY DAY")]
    public void MySqlExpressionTails_KeepRealImplicitAliases(string expression)
    {
        string sql = BuildExpressionTail(expression).Compile(SqlDialect.MySql);
        Assert.DoesNotContain("WITH ", sql); // Both genuine aliases are unique; renaming is unnecessary.
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer)]
    [InlineData(SqlDialect.MySql)]
    public void SyntheticProjectionNames_AvoidExplicitOutputNames(SqlDialect dialect)
    {
        var query = BuildSyntheticCollision();
        string sql = query.Compile(dialect);
        Assert.Contains(SqlIdentifier.Quote(dialect, "dbx_column_1") + " AS " + SqlIdentifier.Quote(dialect, "dbx_column_1_1"), sql);
    }

    [Theory]
    [InlineData("8 DIV divisor")]
    [InlineData("8 MOD divisor")]
    [InlineData("BINARY Name")]
    [InlineData("Created + INTERVAL 1 DAY")]
    [InlineData("Created + INTERVAL (divisor + 1) HOUR")]
    [InlineData("Name REGEXP _utf8mb4'x'")]
    [InlineData("Name RLIKE _utf8mb4'x'")]
    [InlineData("DATE '2026-10-03'")]
    [InlineData("X'78'")]
    [InlineData("B'01'")]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlUnnamedExpressionWrapper_RemainsComposable(string expression)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL database.");
        using var client = new DBAClientX.MySql();
        var query = new Query().Select("q.divisor").From(BuildExpressionTail(expression), "q");
        var rows = await client.QueryAsListAsync(connection!, query.Compile(SqlDialect.MySql), row => Convert.ToInt64(row.GetValue(0)));
        Assert.Equal(2L, Assert.Single(rows));
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer)]
    [InlineData(SqlDialect.MySql)]
    [Trait("Category", "LiveProvider")]
    public async Task SyntheticProjectionNames_RemainComposable(SqlDialect dialect)
    {
        string? connection = Environment.GetEnvironmentVariable(dialect == SqlDialect.SqlServer
            ? "DBACLIENTX_SQLSERVER_TEST_CONNECTION" : "DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set the provider's test connection.");
        var query = new Query().Select("q.dbx_column_1").From(BuildSyntheticCollision(), "q");
        if (dialect == SqlDialect.SqlServer)
        {
            using var client = new DBAClientX.SqlServer();
            var rows = await client.QueryAsListAsync(connection!, query.Compile(dialect), row => Convert.ToInt64(row.GetValue(0)));
            Assert.Equal(1L, Assert.Single(rows));
        }
        else
        {
            using var client = new DBAClientX.MySql();
            var rows = await client.QueryAsListAsync(connection!, query.Compile(dialect), row => Convert.ToInt64(row.GetValue(0)));
            Assert.Equal(1L, Assert.Single(rows));
        }
    }

    private static Query BuildExpressionTail(string expression)
    {
        Query Operand() => new Query().SelectRaw(expression + ", divisor")
            .FromRaw("(SELECT 2 AS divisor, 'x' AS Name, '2026-10-03' AS Created) AS n");
        return Operand().Union(Operand()).Intersect(Operand());
    }

    private static Query BuildSyntheticCollision() => new Query().SelectRaw("1 AS dbx_column_1, 2")
        .Union(new Query().SelectRaw("3, 4")).Intersect(new Query().SelectRaw("1, 2"));

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
    [InlineData("regex")]
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
        "regex" => new Query().SelectRaw("Name REGEXP 'x', Code RLIKE 'x'").FromRaw("(SELECT 'x' AS Name, 'x' AS Code) AS numbers")
            .Union(new Query().SelectRaw("1, 1")).Intersect(new Query().SelectRaw("1, 1")),
        _ => throw new ArgumentOutOfRangeException(nameof(shape))
    };

    private static void AssertRows(string shape, IReadOnlyList<ProjectionRow> rows)
    {
        if (shape == "regex")
        {
            var row = Assert.Single(rows);
            Assert.Equal(new[] { "dbx_column_0", "dbx_column_1" }, row.Names);
            Assert.Equal(new long?[] { 1, 1 }, row.Values);
        }
        else if (shape is "duplicate" or "nested")
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
