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
    [InlineData("hint")]
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
            "hint" => new Query().Select("o.Id").FromRaw("numbers USE INDEX (ix)")
                .Union(new Query().Select("3")).Intersect(new Query().Select("1")),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        var query = new Query().Select("o.Id").From("outer_numbers", "o").WhereIn("o.Id", child);
        var error = Assert.Throws<NotSupportedException>(() => query.CompileWithParameters(SqlDialect.MySql));
        Assert.Contains("outer qualifier 'o'", error.Message);
        Assert.Contains("move the correlation", error.Message);
    }

    [Theory]
    [InlineData("straight_join")]
    [InlineData("join")]
    [InlineData("from")]
    [InlineData("apply")]
    [InlineData("with")]
    [InlineData("union")]
    public void MySqlQualifiedKeywordColumns_DoNotInventSources(string column)
    {
        var operand = new Query().SelectRaw($"n.{column} LIKE o.Name AS Matched").FromRaw("numbers AS n")
            .Union(new Query().Select("1")).Intersect(new Query().Select("1"));
        var outer = new Query().Select("o.Id").From("outer_numbers", "o").WhereIn("o.Id", operand);
        var error = Assert.Throws<NotSupportedException>(() => outer.Compile(SqlDialect.MySql));
        Assert.Contains("outer qualifier 'o'", error.Message);
    }

    [Theory]
    [InlineData("numbers USE INDEX (ix)", "numbers")]
    [InlineData("numbers FORCE KEY (ix)", "numbers")]
    [InlineData("numbers IGNORE INDEX (ix)", "numbers")]
    [InlineData("numbers USE INDEX ()", "numbers")]
    [InlineData("numbers AS n USE INDEX FOR JOIN (ix)", "n")]
    [InlineData("numbers AS n FORCE INDEX FOR ORDER BY (ix)", "n")]
    [InlineData("numbers AS n IGNORE KEY FOR GROUP BY (ix)", "n")]
    [InlineData("numbers USE INDEX (ix) IGNORE INDEX FOR ORDER BY (ix), other AS j", "j")]
    [InlineData("numbers AS n USE INDEX FOR JOIN (ix) JOIN other AS j ON n.Id=j.Id", "j")]
    [InlineData("numbers PARTITION (p0)", "numbers")]
    [InlineData("numbers PARTITION (p0, p1) AS n USE INDEX FOR GROUP BY (ix)", "n")]
    [InlineData("numbers AS n STRAIGHT_JOIN other AS j ON n.Id=j.Id", "j")]
    [InlineData("numbers STRAIGHT_JOIN other AS j ON numbers.Id=j.Id", "j")]
    [InlineData("{ OJ numbers AS n LEFT JOIN other AS j ON n.Id=j.Id }", "n")]
    public void MySqlGroupedSourceModifiers_PreserveLocalBindings(string source, string qualifier)
    {
        var first = new Query().Select(qualifier + ".Id").FromRaw(source);
        string sql = first.Union(new Query().Select("1")).Intersect(new Query().Select("1")).Compile(SqlDialect.MySql);
        Assert.Contains(source, sql);
        Assert.Contains("dbx_left_", sql);
    }

    [Theory]
    [InlineData("numbers FOR SYSTEM_TIME ALL AS n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME ALL n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-01' AS n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME AS OF NOW() AS n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME AS OF CURRENT_TIMESTAMP + INTERVAL 1 SECOND n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME BETWEEN TIMESTAMP '2020-01-01' AND NOW() AS n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME FROM '2020-01-01' TO NOW() AS n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME AS OF TRANSACTION 42 AS n", "n")]
    [InlineData("numbers FOR SYSTEM_TIME ALL AS n, other AS j", "j")]
    [InlineData("(numbers FOR SYSTEM_TIME ALL AS n JOIN other AS j ON n.Id=j.Id)", "j")]
    [InlineData("(SELECT Id FROM numbers) FOR SYSTEM_TIME ALL AS n", "n")]
    [InlineData("numbers AS n JOIN other FOR SYSTEM_TIME ALL AS j ON n.Id=j.Id", "j")]
    public void MariaDbTemporalSourceModifiers_PreserveLocalBindings(string source, string qualifier)
    {
        var first = new Query().Select(qualifier + ".Id").FromRaw(source);
        Assert.Contains(source, first.Union(new Query().Select("1")).Intersect(new Query().Select("1")).Compile(SqlDialect.MySql));
    }

    [Theory]
    [InlineData("/*! AS n */")]
    [InlineData("/*!80000 AS n */")]
    [InlineData("/*M! AS n */")]
    [InlineData("/*M!100000 AS n */")]
    public void MySqlExecutableSourceComments_DeferVersionDependentBinding(string modifier)
    {
        var first = new Query().Select("n.Id").FromRaw("numbers " + modifier);
        Assert.Contains(modifier, first.Union(new Query().Select("1")).Intersect(new Query().Select("1")).Compile(SqlDialect.MySql));
    }

    [Fact]
    public void ExecutableCommentMarkersInsideStrings_DoNotDisableTheCorrelationGuard()
    {
        var first = new Query().Select("o.Id").From("numbers").WhereRaw("'/*! AS n */'", "<>", "");
        Assert.Throws<NotSupportedException>(() => first.Union(new Query().Select("1"))
            .Intersect(new Query().Select("1")).Compile(SqlDialect.MySql));
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
    [InlineData("groupedJoinComma")]
    [InlineData("joinComma")]
    [InlineData("quotedAlias")]
    [InlineData("quotedTable")]
    [InlineData("qualified")]
    [InlineData("defaultDatabase")]
    public void MySqlGroupedSourceSyntax_PreservesLocalBindings(string shape)
    {
        Query Operand() => shape switch
        {
            "grouped" => new Query().Select("n.Id").FromRaw("(numbers AS n)"),
            "groupedJoin" => new Query().Select("j.Id").FromRaw("(numbers AS n JOIN other AS j ON n.Id = j.Id)"),
            "groupedComma" => new Query().Select("j.Id").FromRaw("(numbers AS n, other AS j)"),
            "groupedJoinComma" => new Query().Select("k.Id").FromRaw("(numbers AS n JOIN other AS j ON n.Id = j.Id, third AS k)"),
            "joinComma" => new Query().Select("k.Id").FromRaw("numbers AS n JOIN other AS j ON n.Id = j.Id, third AS k"),
            "quotedAlias" => new Query().Select("order.Id").From("numbers", "order"),
            "quotedTable" => new Query().Select("order.Id").From("order"),
            "qualified" => new Query().Select("app.numbers.Id").From("app.numbers"),
            "defaultDatabase" => new Query().Select("app.numbers.Id").From("numbers"),
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
    [InlineData("groupedComma")]
    [InlineData("groupedJoinComma")]
    [InlineData("joinComma")]
    [InlineData("defaultDatabase")]
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
                    "groupedComma" => new Query().Select("j.Id").FromRaw($"(`order` AS n, `{peer}` AS j)"),
                    "groupedJoinComma" => new Query().Select("k.Id").FromRaw($"(`order` AS n JOIN `{peer}` AS j ON n.Id = j.Id, (SELECT 1 AS Id) AS k)"),
                    "joinComma" => new Query().Select("k.Id").FromRaw($"`order` AS n JOIN `{peer}` AS j ON n.Id = j.Id, (SELECT 1 AS Id) AS k"),
                    "quotedAlias" => new Query().Select("order.Id").From("order", "order"),
                    "quotedTable" => new Query().Select("order.Id").From("order"),
                    "qualified" => new Query().Select(database + ".order.Id").From(database + ".order"),
                    "defaultDatabase" => new Query().Select(database + ".order.Id").From("order"),
                    _ => throw new ArgumentOutOfRangeException(nameof(shape))
                };
                // Each temporary table is read once, respecting MySQL's temporary-table reopening restriction.
                bool usesPeer = shape is "groupedJoin" or "groupedComma" or "groupedJoinComma" or "joinComma";
                var second = usesPeer ? new Query().Select("1") : new Query().Select("Id").From(peer);
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

    [Theory]
    [InlineData("use")]
    [InlineData("force")]
    [InlineData("ignore")]
    [InlineData("empty")]
    [InlineData("join")]
    [InlineData("order")]
    [InlineData("group")]
    [InlineData("multiple")]
    [InlineData("straight")]
    [InlineData("odbc")]
    [Trait("Category", "LiveProvider")]
    public async Task MySqlGroupedIndexModifiers_ExecuteWithLocalBindings(string shape)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL test database.");
        using var client = new MySql();
        string table = "dbx_hint_" + Guid.NewGuid().ToString("N");
        string peer = table + "_peer";
        await client.RunInTransactionAsync(connection!, async (transaction, token) =>
        {
            try
            {
                await transaction.ExecuteNonQueryAsync(connection!, $"CREATE TEMPORARY TABLE `{table}` (Id INT, INDEX ix (Id)); CREATE TEMPORARY TABLE `{peer}` (Id INT); INSERT INTO `{table}` VALUES (1); INSERT INTO `{peer}` VALUES (1);", useTransaction: true, cancellationToken: token);
                string source = shape switch
                {
                    "use" => $"`{table}` USE INDEX (ix)",
                    "force" => $"`{table}` FORCE KEY (ix)",
                    "ignore" => $"`{table}` IGNORE INDEX (ix)",
                    "empty" => $"`{table}` USE INDEX ()",
                    "join" => $"`{table}` AS n USE INDEX FOR JOIN (ix) JOIN `{peer}` AS j ON n.Id=j.Id",
                    "order" => $"`{table}` AS n FORCE INDEX FOR ORDER BY (ix)",
                    "group" => $"`{table}` AS n IGNORE KEY FOR GROUP BY (ix)",
                    "multiple" => $"`{table}` USE INDEX (ix) IGNORE INDEX FOR ORDER BY (ix), `{peer}` AS j",
                    "straight" => $"`{table}` STRAIGHT_JOIN `{peer}` AS j ON `{table}`.Id=j.Id",
                    "odbc" => $"{{ OJ `{table}` AS n LEFT JOIN `{peer}` AS j ON n.Id=j.Id }}",
                    _ => throw new ArgumentOutOfRangeException(nameof(shape))
                };
                string qualifier = shape is "join" or "multiple" or "straight" ? "j" : shape is "order" or "group" or "odbc" ? "n" : table;
                var query = new Query().Select(qualifier + ".Id").FromRaw(source)
                    .Union(new Query().Select("1")).Intersect(new Query().Select("1"));
                var rows = await transaction.QueryAsListAsync(connection!, query.Compile(SqlDialect.MySql),
                    row => Convert.ToInt64(row.GetValue(0)), useTransaction: true, cancellationToken: token);
                Assert.Equal(1L, Assert.Single(rows));
            }
            finally
            {
                await transaction.ExecuteNonQueryAsync(connection!, $"DROP TEMPORARY TABLE IF EXISTS `{table}`, `{peer}`;", useTransaction: true, cancellationToken: token);
            }
        });
    }

    [Theory]
    [InlineData("cte")]
    [InlineData("derived")]
    [InlineData("lateral")]
    [InlineData("jsonTable")]
    [Trait("Category", "LiveProvider")]
    public async Task MySql8GroupedInternalDefinitions_PreserveEligibleBindings(string shape)
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL test database.");
        using var client = new MySql();
        string version = Convert.ToString(await client.ExecuteScalarAsync(connection!, "SELECT VERSION();"))!;
        Assert.SkipWhen(version.Contains("MariaDB", StringComparison.OrdinalIgnoreCase),
            "MariaDB does not share MySQL's ancestor-correlation and LATERAL contracts.");
        var operand = shape switch
        {
            "cte" => new Query().SelectRaw("(WITH c AS (SELECT n.Id AS Id) SELECT c.Id FROM c) AS Id")
                .FromRaw("(SELECT 1 AS Id) AS n"),
            "derived" => new Query().SelectRaw("(SELECT q.Id FROM (SELECT n.Id AS Id) AS q) AS Id")
                .FromRaw("(SELECT 1 AS Id) AS n"),
            "lateral" => new Query().Select("l.Id").FromRaw("(SELECT 1 AS Id) AS n").JoinRaw("LATERAL (SELECT n.Id) AS l", "1 = 1"),
            "jsonTable" => new Query().Select("o.Id").FromRaw("(SELECT '[{\"Id\":1}]' AS Json) AS n")
                .JoinRaw("JSON_TABLE(n.Json, '$[*]' COLUMNS (Id INT PATH '$.Id')) AS o", "1 = 1"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        var rows = await client.QueryAsListAsync(connection!, operand.Union(new Query().Select("1"))
            .Intersect(new Query().Select("1")).Compile(SqlDialect.MySql), row => Convert.ToInt64(row.GetValue(0)));
        Assert.Equal(1L, Assert.Single(rows));
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

    [Theory]
    [InlineData("cte")]
    [InlineData("derived")]
    public void MySqlDefinitions_CanSeeEligibleAncestorRelations(string shape)
    {
        string expression = shape == "cte" ? "(WITH c AS (SELECT n.Id AS Id) SELECT c.Id FROM c) AS Id"
            : "(SELECT q.Id FROM (SELECT n.Id AS Id) AS q) AS Id";
        Assert.Contains("dbx_left_", new Query().SelectRaw(expression).From("numbers", "n")
            .Union(new Query().Select("1")).Intersect(new Query().Select("1")).Compile(SqlDialect.MySql));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MySqlJsonTableArguments_CannotBindToTheirResultAliasOrLaterSources(bool forward)
    {
        string qualifier = forward ? "j" : "o";
        var operand = new Query().Select("o.Id").From("numbers", "n")
            .JoinRaw($"JSON_TABLE({qualifier}.Json, '$[*]' COLUMNS (Id INT PATH '$.Id')) AS o", "1 = 1");
        if (forward) operand.Join("other", "j", "j.Id", "=", "n.Id");
        Assert.Throws<NotSupportedException>(() => operand.Union(new Query().Select("1")).Intersect(new Query().Select("1"))
            .Compile(SqlDialect.MySql));
    }

    [Fact]
    public void MySqlJsonTableArguments_CanBindToEarlierSources()
    {
        var operand = new Query().Select("o.Id").From("numbers", "n")
            .JoinRaw("JSON_TABLE(n.Json, '$[*]' COLUMNS (Id INT PATH '$.Id')) AS o", "1 = 1");
        Assert.Contains("dbx_left_", operand.Union(new Query().Select("1")).Intersect(new Query().Select("1"))
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
