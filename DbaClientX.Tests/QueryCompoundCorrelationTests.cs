using DBAClientX;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

public sealed class QueryCompoundCorrelationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlMixedUnion_PreservesOuterReferences(bool reverse)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL database.");
        using var client = new MySql();
        var rows = await client.QueryAsListAsync(connection!, MixedUnion(reverse).Compile(SqlDialect.MySql),
            row => Convert.ToInt64(row.GetValue(0)));
        Assert.Equal(new long[] { 1, 2 }, rows);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MixedUnion_DoesNotIntroduceADerivedScope(bool reverse)
    {
        foreach (var dialect in Enum.GetValues<SqlDialect>())
            Assert.DoesNotContain("dbx_left_", MixedUnion(reverse).Compile(dialect));
    }

    [Theory]
    [InlineData("mixed")]
    [InlineData("nested")]
    [InlineData("paged")]
    [InlineData("join")]
    public void MySqlGroupedCorrelation_HasAnActionableCompilationError(string shape)
    {
        Query Correlated() => new Query().SelectRaw("o.Id");
        Query child = shape switch
        {
            "mixed" => Correlated().Union(new Query().Select("3")).Intersect(new Query().Select("1")),
            "nested" => new Query().Select("3").Union(Correlated().Union(new Query().Select("1"))),
            "paged" => new Query().Select("3").Union(Correlated().Limit(1)),
            "join" => new Query().Select("n.Id").From("numbers", "n").Join("other", "j", "j.Id", "=", "o.Id")
                .Union(new Query().Select("3")).Intersect(new Query().Select("1")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        var query = new Query().Select("o.Id").From("outer_numbers", "o").WhereIn("o.Id", child);
        var error = Assert.Throws<NotSupportedException>(() => query.CompileWithParameters(SqlDialect.MySql));
        Assert.Contains("outer qualifier 'o'", error.Message);
        Assert.Contains("move the correlation", error.Message);
    }

    [Fact]
    public void MySqlGroupedLocalAliases_AreResolvedWithinEachOperand()
    {
        Query Operand() => new Query().SelectRaw("o.Id, 'outside.Id' AS Text /* outside.Id */")
            .From("app.numbers", "o").Join("app.other", "j", "j.Id", "=", "o.Id");
        var nested = new Query().Select("q.Id", "q.Text").From(Operand(), "q");
        var query = Operand().Union(Operand()).Intersect(nested);
        Assert.Contains("dbx_left_", query.CompileWithParameters(SqlDialect.MySql).Sql);
    }

    [Theory]
    [InlineData("grouped")]
    [InlineData("groupedJoin")]
    [InlineData("groupedComma")]
    [InlineData("quotedAlias")]
    [InlineData("quotedTable")]
    [InlineData("qualified")]
    public void MySqlGroupedSourceSyntax_PreservesLocalBindings(string shape)
    {
        Query Operand() => shape switch
        {
            "grouped" => new Query().Select("n.Id").FromRaw("(numbers AS n)"),
            "groupedJoin" => new Query().Select("j.Id").FromRaw("(numbers AS n JOIN other AS j ON n.Id = j.Id)"),
            "groupedComma" => new Query().Select("j.Id").FromRaw("(numbers AS n, other AS j)"),
            "quotedAlias" => new Query().Select("order.Id").From("numbers", "order"),
            "quotedTable" => new Query().Select("order.Id").From("order"),
            "qualified" => new Query().Select("app.numbers.Id").From("app.numbers"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        Assert.Contains("dbx_left_", Operand().Union(Operand()).Intersect(new Query().Select("1")).Compile(SqlDialect.MySql));
    }

    [Theory]
    [InlineData("grouped")]
    [InlineData("groupedJoin")]
    [InlineData("quotedAlias")]
    [InlineData("quotedTable")]
    [InlineData("qualified")]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlGroupedSourceSyntax_ExecutesWithLocalBindings(string shape)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL test database.");
        using var client = new MySql();
        string peer = "dbx_scope_" + Guid.NewGuid().ToString("N");
        await client.RunInTransactionAsync(connection!, async (transaction, token) =>
        {
            try
            {
                await transaction.ExecuteNonQueryAsync(connection!, $"CREATE TEMPORARY TABLE `order` (Id INT); CREATE TEMPORARY TABLE `{peer}` (Id INT); INSERT INTO `order` VALUES (1); INSERT INTO `{peer}` VALUES (1);", useTransaction: true, cancellationToken: token);
                string database = Convert.ToString(await transaction.ExecuteScalarAsync(connection!, "SELECT DATABASE();", useTransaction: true, cancellationToken: token))!;
                Query first = shape switch
                {
                    "grouped" => new Query().Select("n.Id").FromRaw("(`order` AS n)"),
                    "groupedJoin" => new Query().Select("j.Id").FromRaw($"(`order` AS n JOIN `{peer}` AS j ON n.Id = j.Id)"),
                    "quotedAlias" => new Query().Select("order.Id").From("order", "order"),
                    "quotedTable" => new Query().Select("order.Id").From("order"),
                    "qualified" => new Query().Select(database + ".order.Id").From(database + ".order"),
                    _ => throw new ArgumentOutOfRangeException(nameof(shape))
                };
                // Each temporary table is read once, respecting MySQL's temporary-table reopening restriction.
                var second = shape == "groupedJoin" ? new Query().Select("1") : new Query().Select("Id").From(peer);
                var rows = await transaction.QueryAsListAsync(connection!, first.Union(second).Intersect(new Query().Select("1"))
                    .Compile(SqlDialect.MySql), row => Convert.ToInt64(row.GetValue(0)), useTransaction: true, cancellationToken: token);
                Assert.Equal(1L, Assert.Single(rows));
            }
            finally
            {
                await transaction.ExecuteNonQueryAsync(connection!, $"DROP TEMPORARY TABLE IF EXISTS `order`, `{peer}`;", useTransaction: true, cancellationToken: token);
            }
        });
    }

    [Fact]
    public void MySqlGroupedExternalQualifiedRelation_IsNotHiddenByALocalTableName()
    {
        var operand = new Query().Select("outer_db.numbers.Id").From("inner_db.numbers");
        Assert.Throws<NotSupportedException>(() => operand.Union(new Query().Select("1")).Intersect(new Query().Select("1"))
            .Compile(SqlDialect.MySql));
    }

    [Theory]
    [InlineData("derived")]
    [InlineData("cte")]
    [InlineData("lateralSelf")]
    [InlineData("lateralForward")]
    public void MySqlDefinitions_DoNotSeeTheirResultAliasOrLaterSources(string shape)
    {
        var operand = shape switch
        {
            "derived" => new Query().Select("o.Id").From(new Query().Select("o.Id"), "o"),
            "cte" => new Query().Select("q.Id").FromRaw("(WITH c AS (SELECT n.Id) SELECT 1 AS Id FROM numbers AS n) AS q"),
            "lateralSelf" => new Query().Select("l.Id").From("numbers", "n").JoinRaw("LATERAL (SELECT l.Id) AS l", "1 = 1"),
            "lateralForward" => new Query().Select("l.Id").From("numbers", "n")
                .JoinRaw("LATERAL (SELECT j.Id) AS l", "1 = 1").Join("other", "j", "j.Id", "=", "n.Id"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        Assert.Throws<NotSupportedException>(() => operand.Union(new Query().Select("1")).Intersect(new Query().Select("1"))
            .Compile(SqlDialect.MySql));
    }

    [Fact]
    public void MySqlGroupedNestedCorrelation_ToAnInternalScope_RemainsValid()
    {
        var localChild = new Query().Select("j.Id").From("other", "j").WhereRaw("j.Id = n.Id AND 1", "=", 1);
        Query Operand() => new Query().Select("n.Id").From("numbers", "n").WhereIn("n.Id", localChild);
        Assert.Contains("dbx_left_", Operand().Union(Operand()).Intersect(new Query().Select("1"))
            .CompileWithParameters(SqlDialect.MySql).Sql);
    }

    [Fact]
    public void MySqlGroupedLateralSource_ResolvesItsLocalAlias()
    {
        Query Operand() => new Query().Select("l.Id").From("numbers", "n")
            .JoinRaw("LATERAL (SELECT n.Id) AS l", "1 = 1");
        Assert.Contains("dbx_left_", Operand().Union(Operand()).Intersect(new Query().Select("1")).Compile(SqlDialect.MySql));
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlGroupedNestedCorrelation_ToAnInternalScope_Executes()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL database.");
        var child = new Query().Select("1").WhereRaw("n.Id", "=", 1);
        Query Operand() => new Query().Select("n.Id").FromRaw("(SELECT 1 AS Id) AS n").WhereIn("n.Id", child);
        using var client = new MySql();
        var rows = await client.QueryAsListAsync(connection!, Operand().Union(Operand()).Intersect(new Query().Select("1"))
            .Compile(SqlDialect.MySql), row => Convert.ToInt64(row.GetValue(0)));
        Assert.Equal(1L, Assert.Single(rows));
    }

    private static Query MixedUnion(bool reverse)
    {
        var child = new Query().SelectRaw("o.Id");
        if (reverse) child.UnionAll(new Query().Select("3")).Union(new Query().Select("3"));
        else child.Union(new Query().Select("3")).UnionAll(new Query().Select("3"));
        return new Query().Select("o.Id").FromRaw("(SELECT 1 AS Id UNION ALL SELECT 2) AS o")
            .WhereIn("o.Id", child).OrderBy("o.Id");
    }
}
