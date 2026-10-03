using System.Data;
using System.Globalization;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QueryCompoundExecutableCommentTests
{
    [Theory]
    [InlineData("1 /*! , 2 */")]
    [InlineData("1 /*!99999 , 2 */")]
    [InlineData("1 /*M! , 2 */")]
    [InlineData("1 /*M!999999 , 2 */")]
    [InlineData("1 /*! AS FirstValue, 2 AS SecondValue */")]
    [InlineData("1 /*! AS FirstValue */")]
    public void MySqlExecutableProjection_DefersServerDependentWidthAndNames(string projection)
    {
        string sql = Compound(projection).Compile(SqlDialect.MySql);
        Assert.Contains("SELECT * FROM (SELECT " + projection, sql);
        Assert.DoesNotContain("WITH ", sql);
    }

    [Theory]
    [InlineData("1 /* , 2 */")]
    [InlineData("'/*! , 2 */'")]
    [InlineData("'/*M! , 2 */'")]
    public void OrdinaryCommentOrString_DoesNotMakeProjectionWidthUnknown(string projection)
    {
        string sql = Compound(projection).Compile(SqlDialect.MySql);
        Assert.Contains("WITH `dbx_left_1_source` (`dbx_column_0`)", sql);
    }

    [Theory]
    [InlineData("1 /*! , 2 */")]
    [InlineData("1 /*!99999 , 2 */")]
    [InlineData("1 /*M! , 2 */")]
    [InlineData("1 /*M!999999 , 2 */")]
    [InlineData("1 /*! AS FirstValue, 2 AS SecondValue */")]
    [InlineData("1 /*! AS FirstValue */")]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlExecutableProjection_PreservesServerValuesAndMetadata(string projection)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to an isolated MySQL/MariaDB test database.");
        using var client = new DBAClientX.MySql();
        var baseline = await client.QueryAsListAsync(connection!, new Query().SelectRaw(projection).Compile(SqlDialect.MySql), ReadRow);
        var wrapped = await client.QueryAsListAsync(connection!, Compound(projection).Compile(SqlDialect.MySql), ReadRow);
        var expected = Assert.Single(baseline);
        var actual = Assert.Single(wrapped);
        Assert.Equal(expected.Names, actual.Names);
        Assert.Equal(expected.Values, actual.Values);
    }

    private static Query Compound(string projection)
        => new Query().SelectRaw(projection).Union(new Query().SelectRaw(projection))
            .Intersect(new Query().SelectRaw(projection));

    private static (string[] Names, string[] Values) ReadRow(IDataRecord row)
        => (Enumerable.Range(0, row.FieldCount).Select(row.GetName).ToArray(),
            Enumerable.Range(0, row.FieldCount).Select(index => Convert.ToString(row.GetValue(index), CultureInfo.InvariantCulture)!).ToArray());
}
