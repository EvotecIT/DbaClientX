using DBAClientX;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

[Collection(nameof(QueryCompilerCacheCollection))]
public sealed class QueryCompoundPagingTests : IDisposable
{
    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-compound-" + Guid.NewGuid().ToString("N") + ".db");
    private readonly SQLite _sqlite = new();

    public QueryCompoundPagingTests()
        => _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Numbers (Id INTEGER); INSERT INTO Numbers VALUES (1),(2),(3),(4)");

    [Theory]
    [InlineData(SqlDialect.SQLite, "SELECT \"Id\" FROM \"Numbers\" ORDER BY \"Id\" LIMIT -1 OFFSET 1")]
    [InlineData(SqlDialect.MySql, "SELECT `Id` FROM `Numbers` ORDER BY `Id` LIMIT 18446744073709551615 OFFSET 1")]
    [InlineData(SqlDialect.PostgreSql, "SELECT \"Id\" FROM \"Numbers\" ORDER BY \"Id\" OFFSET 1")]
    [InlineData(SqlDialect.SqlServer, "SELECT [Id] FROM [Numbers] ORDER BY [Id] OFFSET 1 ROWS")]
    [InlineData(SqlDialect.Oracle, "SELECT \"Id\" FROM \"Numbers\" ORDER BY \"Id\" OFFSET 1 ROWS")]
    public void OffsetWithoutLimit_UsesValidDialectSyntax(SqlDialect dialect, string expected)
        => Assert.Equal(expected, new Query().Select("Id").From("Numbers").OrderBy("Id").Offset(1).Compile(dialect));

    [Fact]
    public void OffsetWithoutLimit_ReturnsAllRemainingRows()
        => Assert.Equal(new long[] { 2, 3, 4 }, Execute(SelectNumbers().OrderBy("Id").Offset(1)));

    [Theory]
    [InlineData(SqlDialect.SQLite, "SELECT \"Id\" FROM \"Numbers\" UNION SELECT \"Id\" FROM \"Other\" ORDER BY \"Id\" LIMIT 2 OFFSET 1")]
    [InlineData(SqlDialect.MySql, "SELECT `Id` FROM `Numbers` UNION SELECT `Id` FROM `Other` ORDER BY `Id` LIMIT 2 OFFSET 1")]
    [InlineData(SqlDialect.PostgreSql, "SELECT \"Id\" FROM \"Numbers\" UNION SELECT \"Id\" FROM \"Other\" ORDER BY \"Id\" LIMIT 2 OFFSET 1")]
    [InlineData(SqlDialect.SqlServer, "SELECT [Id] FROM [Numbers] UNION SELECT [Id] FROM [Other] ORDER BY [Id] OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")]
    [InlineData(SqlDialect.Oracle, "SELECT \"Id\" FROM \"Numbers\" UNION SELECT \"Id\" FROM \"Other\" ORDER BY \"Id\" OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY")]
    public void CompoundOrderingAndPaging_ApplyToCombinedResult(SqlDialect dialect, string expected)
    {
        var query = SelectNumbers().OrderBy("Id").Limit(2).Offset(1)
            .Union(new Query().Select("Id").From("Other"));
        Assert.Equal(expected, query.Compile(dialect));
    }

    [Fact]
    public void CompoundOrderingAndPaging_ReturnTheRequestedCombinedPage()
    {
        var query = SelectNumbers().Where("Id", "<=", 2).OrderByDescending("Id").Limit(2).Offset(1)
            .Union(SelectNumbers().Where("Id", ">", 2));
        Assert.Equal(new long[] { 3, 2 }, Execute(query));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void SqlServerCompoundLimit_LimitsTheCombinedResult(bool top)
    {
        var query = SelectNumbers().Union(new Query().Select("Id").From("Other")).OrderBy("Id");
        if (top) query.Top(2); else query.Limit(2);
        Assert.Equal("SELECT TOP 2 * FROM (SELECT [Id] FROM [Numbers] UNION SELECT [Id] FROM [Other]) AS [dbx_compound] ORDER BY [Id]", query.Compile());
    }

    [Fact]
    public void SqlServerZeroLimit_WithOffset_ProducesAnEmptySelection()
        => Assert.Equal("SELECT TOP 0 [Id] FROM [Numbers] ORDER BY [Id]", SelectNumbers().OrderBy("Id").Limit(0).Offset(1).Compile());

    [Fact]
    public void OperandOrderingAndLimit_StayInsideTheOperand()
    {
        var query = SelectNumbers().Where("Id", 1)
            .UnionAll(SelectNumbers().OrderByDescending("Id").Limit(1))
            .OrderBy("Id");
        Assert.Equal(new long[] { 1, 4 }, Execute(query));
    }

    [Fact]
    public void NestedCompound_PreservesItsOwnGrouping()
    {
        var right = SelectNumbers().Where("Id", 2).Union(SelectNumbers().Where("Id", 3))
            .Intersect(SelectNumbers().Where("Id", 3));
        Assert.Equal(new long[] { 1, 3 }, Execute(SelectNumbers().Where("Id", 1).Union(right).OrderBy("Id")));
    }

    [Fact]
    public void MixedSetOperators_ComposeFromLeftToRightOnEveryDialect()
    {
        var query = SelectNumbers().Where("Id", 1).Union(SelectNumbers().Where("Id", 2))
            .Intersect(SelectNumbers().Where("Id", 2)).OrderBy("Id");
        Assert.Equal(new long[] { 2 }, Execute(query));
        foreach (var dialect in Enum.GetValues<SqlDialect>())
        {
            string sql = query.Compile(dialect);
            // SQLite's native result above proves its natural left association. Oracle has the
            // same precedence; the remaining providers need an explicit prefix grouping.
            if (dialect is SqlDialect.SQLite or SqlDialect.Oracle) continue;
            Assert.StartsWith("SELECT * FROM (", sql);
            Assert.True(sql.IndexOf("UNION", StringComparison.Ordinal) < sql.IndexOf(")", StringComparison.Ordinal));
            Assert.True(sql.IndexOf(")", StringComparison.Ordinal) < sql.IndexOf("INTERSECT", StringComparison.Ordinal));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MixedUnionChains_PreserveDuplicateSemantics(bool distinctLast)
    {
        var query = SelectNumbers().Where("Id", 1);
        if (distinctLast) query.UnionAll(SelectNumbers().Where("Id", 1)).Union(SelectNumbers().Where("Id", 2));
        else query.Union(SelectNumbers().Where("Id", 2)).UnionAll(SelectNumbers().Where("Id", 1));
        Assert.Equal(distinctLast ? new long[] { 1, 2 } : new long[] { 1, 1, 2 }, Execute(query.OrderBy("Id")));
    }

    [Fact]
    public void ParameterizedCompounds_KeepValueOrderAcrossCacheHits()
    {
        QueryCompiler.ClearCache();
        Query Build(int first, int second) => SelectNumbers().Where("Id", first)
            .UnionAll(SelectNumbers().Where("Id", second).OrderBy("Id").Limit(1)).OrderBy("Id");
        var first = Build(1, 4).CompileWithParameters(SqlDialect.SQLite);
        var second = Build(2, 3).CompileWithParameters(SqlDialect.SQLite);

        Assert.Equal(first.Sql, second.Sql);
        Assert.Equal(new object[] { 1, 4 }, first.Parameters);
        Assert.Equal(new object[] { 2, 3 }, second.Parameters);
        Assert.Equal(new long[] { 2, 3 }, Execute(Build(2, 3)));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CompoundMutation_IsRejectedBeforeExecution(bool rootMutation)
    {
        var mutation = new Query().DeleteFrom("Numbers");
        var query = rootMutation ? mutation.Union(SelectNumbers()) : SelectNumbers().Union(mutation);
        Assert.Throws<InvalidOperationException>(() => query.Compile());
    }

    [Fact]
    public void SqlServerOrderedOperand_WithoutPaging_HasAnActionableError()
    {
        var query = SelectNumbers().Union(SelectNumbers().OrderBy("Id"));
        Assert.Contains("Order the combined query", Assert.Throws<InvalidOperationException>(() => query.Compile()).Message);
    }

    private static Query SelectNumbers() => new Query().Select("Id").From("Numbers");

    private IReadOnlyList<long> Execute(Query query)
    {
        var (sql, parameters) = query.CompileWithNamedParameters(SqlDialect.SQLite);
        using var session = _sqlite.OpenSession(_database);
        return session.QueryAsList(sql, row => row.GetInt64(0), parameters);
    }

    public void Dispose()
    {
        _sqlite.Dispose();
        if (File.Exists(_database)) File.Delete(_database);
    }
}
