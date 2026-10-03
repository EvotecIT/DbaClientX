using System.Data;
using System.Globalization;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QueryProjectionOwnerContractTests
{
    [Theory]
    [InlineData("1 /* outer /* inner */ , 2 */", "dbx_column_0")]
    [InlineData("1 AS Value /* outer /* inner */ , 2 */", "Value")]
    [Trait("Category", "LiveProvider")]
    public async Task SqlServerNestedProjectionComment_PreservesWidth(string expression, string name)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server test database.");
        using var client = new DBAClientX.SqlServer();
        var baseline = Assert.Single(await client.QueryAsListAsync(connection!, new Query().SelectRaw(expression).Compile(), ReadRow));
        Assert.Equal(new[] { "1" }, baseline.Values);
        var query = new Query().SelectRaw(expression).Union(new Query().SelectRaw(expression)).Limit(1);
        var result = Assert.Single(await client.QueryAsListAsync(connection!, query.Compile(), ReadRow));
        Assert.Equal(new[] { name }, result.Names);
        Assert.Equal(new[] { "1" }, result.Values);
    }

    [Fact]
    public void SqlServerNestedComment_DoesNotChangeProjectionOrStatementCount()
    {
        const string expression = "1 /* outer /* inner */ , 2 ; SELECT 3 */";
        var query = new Query().SelectRaw(expression).Union(new Query().SelectRaw("1")).Limit(1);
        Assert.Equal("SELECT TOP 1 [dbx_column_0] FROM (SELECT " + expression +
            " UNION SELECT 1) AS [dbx_compound] ([dbx_column_0])", query.Compile());
        Assert.Single(DBAClientX.QueryPlans.SqlStatementText.Split("SELECT " + expression, SqlDialect.SqlServer));
    }

    [Theory]
    [InlineData("1 1st", false)]
    [InlineData("1 1_foo", false)]
    [InlineData("1 1$foo", false)]
    [InlineData("'a' 'b'", true)]
    [InlineData("N'a' 'b'", true)]
    [InlineData("\"a\" \"b\"", true)]
    [InlineData("'a' \"b\"", true)]
    [InlineData("'a' 'b' OutputValue", false)]
    [InlineData("1 AS 'a\\nb'", false)]
    [InlineData("1 'a\\tb'", false)]
    [InlineData("1 AS \"a\\rb\"", false)]
    [InlineData("1 AS 'a\\%b'", false)]
    [InlineData("1 AS 'a\\_b'", false)]
    [InlineData("1e3", true)]
    [InlineData("0x1A", true)]
    [InlineData("0b01", true)]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlProjectionTokens_PreserveNamesAndValues(string expression, bool anonymous)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL/MariaDB test database.");
        using var client = new DBAClientX.MySql();
        Query Operand() => new Query().SelectRaw(expression + ", 2 AS divisor");
        // The anonymous companion forces naming even when the first projection has a genuine alias.
        Query ForcedOperand() => new Query().SelectRaw(expression + ", 2");
        var baseline = Assert.Single(await client.QueryAsListAsync(connection!, Operand().Compile(SqlDialect.MySql), ReadRow));
        var compound = ForcedOperand().Union(ForcedOperand()).Intersect(ForcedOperand());
        var actual = Assert.Single(await client.QueryAsListAsync(connection!, compound.Compile(SqlDialect.MySql), ReadRow));
        Assert.Equal(anonymous ? "dbx_column_0" : baseline.Names[0], actual.Names[0]);
        Assert.Equal("dbx_column_1", actual.Names[1]);
        Assert.Equal(baseline.Values, actual.Values);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlDigitLeadingQualifiedSourceAndColumn_RemainComposable()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL/MariaDB test database.");
        using var client = new DBAClientX.MySql();
        Query Operand() => new Query().SelectRaw("1_foo.1st, 2").FromRaw("(SELECT 1 AS `1st`) AS 1_foo");
        var query = new Query().Select("q.1st").From(Operand().Union(Operand()).Intersect(Operand()), "q");
        var actual = Assert.Single(await client.QueryAsListAsync(connection!, query.Compile(SqlDialect.MySql), ReadRow));
        Assert.Equal(new[] { "1st" }, actual.Names);
        Assert.Equal(new[] { "1" }, actual.Values);
    }

    private static (string[] Names, string[] Values) ReadRow(IDataRecord row)
        => (Enumerable.Range(0, row.FieldCount).Select(row.GetName).ToArray(),
            Enumerable.Range(0, row.FieldCount).Select(index => Convert.ToString(row.GetValue(index), CultureInfo.InvariantCulture)!).ToArray());
}
