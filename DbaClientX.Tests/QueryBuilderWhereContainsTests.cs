using DBAClientX.QueryBuilder;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection(nameof(QueryCompilerCacheCollection))]
public sealed class QueryBuilderWhereContainsTests
{
    /// <summary>Values whose pattern characters must match only themselves.</summary>
    private static readonly string[] Values =
    {
        "50% off", "500 off", "a_b", "axb", "wow!", "wow", "[abc]", "abc", "x*y?", "O'Brien", "back\\slash", "ZAŻÓŁĆ", "zażółć", "Lab", "lab"
    };

    private static readonly string[] Needles = { "%", "0%", "_", "!", "[", "[abc]", "*", "?", "'", "\\", "\\'", "ab", "LAB", "żó", "ŻÓ", "" };

    /// <summary>NUL characters: SQLite's instr() compares them; SQL Server's non-binary collations ignore them.</summary>
    private static readonly string[] SqliteNeedles = Needles.Concat(new[] { "\0", "b\0" }).ToArray();

    [Theory]
    [InlineData(SqlDialect.SqlServer, false, "[Name] LIKE @p0 ESCAPE '!'", "%5![0!%!_!!%")]
    [InlineData(SqlDialect.SqlServer, true, "LOWER([Name]) LIKE LOWER(@p0) ESCAPE '!'", "%5![0!%!_!!%")]
    [InlineData(SqlDialect.PostgreSql, false, "\"Name\" LIKE @p0 ESCAPE '!'", "%5[0!%!_!!%")]
    [InlineData(SqlDialect.PostgreSql, true, "\"Name\" ILIKE @p0 ESCAPE '!'", "%5[0!%!_!!%")]
    [InlineData(SqlDialect.MySql, false, "`Name` LIKE @p0 ESCAPE '!'", "%5[0!%!_!!%")]
    [InlineData(SqlDialect.MySql, true, "LOWER(`Name`) LIKE LOWER(@p0) ESCAPE '!'", "%5[0!%!_!!%")]
    [InlineData(SqlDialect.Oracle, false, "\"Name\" LIKE :p0 ESCAPE '!'", "%5[0!%!_!!%")]
    [InlineData(SqlDialect.Oracle, true, "LOWER(\"Name\") LIKE LOWER(:p0) ESCAPE '!'", "%5[0!%!_!!%")]
    [InlineData(SqlDialect.SQLite, false, "instr(\"Name\", @p0) > 0", "5[0%_!")]
    [InlineData(SqlDialect.SQLite, true, "instr(lower(\"Name\"), lower(@p0)) > 0", "5[0%_!")]
    public void WhereContains_CompilesAnEscapedPatternForEveryDialect(SqlDialect dialect, bool caseInsensitive, string predicate, string parameter)
    {
        QueryCompiler.ClearCache();
        var query = new Query().Select("Id").From("t").WhereContains("Name", "5[0%_!", caseInsensitive);

        var (sql, parameters) = query.CompileWithParameters(dialect);
        var (cachedSql, cachedParameters) = new Query().Select("Id").From("t").WhereContains("Name", "5[0%_!", caseInsensitive).CompileWithParameters(dialect);

        Assert.EndsWith(" WHERE " + predicate, sql);
        Assert.Equal(new object[] { parameter }, parameters);
        Assert.Equal(sql, cachedSql);
        Assert.Equal(parameters, cachedParameters);
    }

    [Fact]
    public void WhereContains_CacheSeparatesCaseModesAndRawExpressions()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.SQLite);

        var sensitive = compiler.CompileWithParameters(new Query().From("t").WhereContains("Name", "a"));
        var insensitive = compiler.CompileWithParameters(new Query().From("t").WhereContains("Name", "a", caseInsensitive: true));
        var raw = compiler.CompileWithParameters(new Query().From("t").OrWhereContainsRaw("lower(Name)", "a"));

        Assert.Equal("SELECT * FROM \"t\" WHERE instr(\"Name\", @p0) > 0", sensitive.Sql);
        Assert.Equal("SELECT * FROM \"t\" WHERE instr(lower(\"Name\"), lower(@p0)) > 0", insensitive.Sql);
        Assert.Equal("SELECT * FROM \"t\" WHERE instr(lower(Name), @p0) > 0", raw.Sql);
        Assert.Equal(new object[] { "a" }, insensitive.Parameters);
    }

    [Fact]
    public void WhereContains_LiteralCompileQuotesThePattern()
    {
        var sql = new Query().From("t").WhereContains("Name", "x' OR 1=1 --%", caseInsensitive: true).Compile(SqlDialect.SqlServer);

        Assert.Equal("SELECT * FROM [t] WHERE LOWER([Name]) LIKE LOWER('%x'' OR 1=1 --!%%') ESCAPE '!'", sql);
    }

    [Fact]
    public void WhereContains_RejectsNullTextAndBlankColumn()
    {
        Assert.Throws<ArgumentException>(() => new Query().WhereContains("Name", null!));
        Assert.Throws<ArgumentException>(() => new Query().WhereContains(" ", "x"));
        Assert.Throws<ArgumentException>(() => new Query().WhereContainsRaw("", "x"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WhereContains_MatchesLikeTheEngineInSqlite(bool caseInsensitive)
    {
        var path = Path.Combine(Path.GetTempPath(), "dbx-contains-" + Guid.NewGuid().ToString("N") + ".db");
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            sqlite.ExecuteNonQuery(path, "CREATE TABLE t (Id INTEGER PRIMARY KEY, Name TEXT)");
            for (var i = 0; i < Values.Length; i++)
            {
                sqlite.ExecuteNonQuery(path, "INSERT INTO t VALUES (@id, @name)", new Dictionary<string, object?> { ["@id"] = i, ["@name"] = Values[i] });
            }

            sqlite.ExecuteNonQuery(path, "INSERT INTO t VALUES (100, NULL)");
            foreach (var needle in SqliteNeedles)
            {
                var (sql, parameters) = new Query().Select("Id").From("t").WhereContains("Name", needle, caseInsensitive).OrderBy("Id")
                    .CompileWithNamedParameters(SqlDialect.SQLite);
                var actual = await sqlite.QueryReadOnlyAsListAsync(path, sql, reader => (int)reader.GetInt64(0), parameters);

                // SQLite's lower() folds ASCII letters only.
                static string Fold(string value) => new string(value.Select(c => c < 128 ? char.ToLowerInvariant(c) : c).ToArray());
                var expected = Enumerable.Range(0, Values.Length)
                    .Where(i => caseInsensitive ? Fold(Values[i]).Contains(Fold(needle), StringComparison.Ordinal) : Values[i].Contains(needle, StringComparison.Ordinal))
                    .ToArray();
                Assert.True(expected.SequenceEqual(actual), $"Needle '{needle}': expected [{string.Join(",", expected)}], got [{string.Join(",", actual)}].");
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
    public async Task WhereContains_CaseInsensitive_MatchesLikeTheEngineInSqlServer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to a SQL Server database.");
        var table = "dbo.DbaxContains" + Guid.NewGuid().ToString("N");
        using var sql = new DBAClientX.SqlServer();
        await sql.ExecuteNonQueryAsync(connectionString!, "CREATE TABLE " + table + " (Id int PRIMARY KEY, Name nvarchar(50) COLLATE Latin1_General_100_CS_AS NULL)");
        try
        {
            for (var i = 0; i < Values.Length; i++)
            {
                await sql.ExecuteNonQueryAsync(connectionString!, "INSERT INTO " + table + " VALUES (@id, @name)", new Dictionary<string, object?> { ["@id"] = i, ["@name"] = Values[i] });
            }

            foreach (var needle in Needles)
            {
                var (text, parameters) = new Query().Select("Id").From(table).WhereContains("Name", needle, caseInsensitive: true).OrderBy("Id")
                    .CompileWithNamedParameters(SqlDialect.SqlServer);
                var actual = await sql.QueryAsListAsync(connectionString!, text, record => record.GetInt32(0), parameters);

                var expected = Enumerable.Range(0, Values.Length)
                    .Where(i => Values[i].ToLowerInvariant().Contains(needle.ToLowerInvariant(), StringComparison.Ordinal))
                    .ToArray();
                Assert.True(expected.SequenceEqual(actual), $"Needle '{needle}': expected [{string.Join(",", expected)}], got [{string.Join(",", actual)}].");
            }
        }
        finally
        {
            await sql.ExecuteNonQueryAsync(connectionString!, "DROP TABLE IF EXISTS " + table);
        }
    }

    [Fact]
    public void WhereContains_InsideWhereNotAndBetweenOtherValues_KeepsParameterOrderOnCacheHits()
    {
        QueryCompiler.ClearCache();
        var compiler = new QueryCompiler(SqlDialect.PostgreSql);
        Query Build(int tenant, string needle, int year) => new Query().From("t").Where("tenant", tenant)
            .WhereNot(q => q.WhereContains("Name", needle, caseInsensitive: true))
            .OrWhereContains("Owner", needle).Where("year", year);

        compiler.CompileWithParameters(Build(1, "a%", 2020));
        var countAfterFirst = QueryCompiler.CacheCount;
        var (sql, parameters) = compiler.CompileWithParameters(Build(2, "b_", 2021));

        Assert.Equal(countAfterFirst, QueryCompiler.CacheCount);
        Assert.Equal("SELECT * FROM \"t\" WHERE \"tenant\" = @p0 AND NOT (\"Name\" ILIKE @p1 ESCAPE '!') OR \"Owner\" LIKE @p2 ESCAPE '!' AND \"year\" = @p3", sql);
        Assert.Equal(new object[] { 2, "%b!_%", "%b!_%", 2021 }, parameters);
    }

    [Theory]
    [InlineData(SqlDialect.MySql)]
    [InlineData(SqlDialect.PostgreSql)]
    public void WhereContains_LiteralCompileWithBackslash_KeepsThePatternInsideItsLiteral(SqlDialect dialect)
    {
        var sql = new Query().From("t").WhereContains("Name", "a\\' OR 1=1 --").Compile(dialect);

        Assert.DoesNotContain("' OR 1=1", sql.Replace("''", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.EndsWith(" ESCAPE '!'", sql);
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task WhereContains_MatchesLikeTheEngineInPostgreSql()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to a PostgreSQL database.");
        var table = "dbax_contains_" + Guid.NewGuid().ToString("N");
        using var postgres = new DBAClientX.PostgreSql();
        await postgres.ExecuteNonQueryAsync(connectionString!, "CREATE TABLE " + table + " (\"Id\" int PRIMARY KEY, \"Name\" text NULL)");
        try
        {
            for (var i = 0; i < Values.Length; i++)
            {
                await postgres.ExecuteNonQueryAsync(connectionString!, "INSERT INTO " + table + " VALUES (@id, @name)", new Dictionary<string, object?> { ["@id"] = i, ["@name"] = Values[i] });
            }

            foreach (var caseInsensitive in new[] { false, true })
            {
                foreach (var needle in Needles)
                {
                    var (text, parameters) = new Query().Select("Id").From(table).WhereContains("Name", needle, caseInsensitive).OrderBy("Id").CompileWithNamedParameters(SqlDialect.PostgreSql);
                    var actual = await postgres.QueryAsListAsync(connectionString!, text, record => record.GetInt32(0), parameters);
                    var expected = Enumerable.Range(0, Values.Length)
                        .Where(i => caseInsensitive ? Values[i].ToLowerInvariant().Contains(needle.ToLowerInvariant(), StringComparison.Ordinal) : Values[i].Contains(needle, StringComparison.Ordinal))
                        .ToArray();
                    Assert.True(expected.SequenceEqual(actual), $"Needle '{needle}' ({caseInsensitive}): expected [{string.Join(",", expected)}], got [{string.Join(",", actual)}].");
                }
            }
        }
        finally
        {
            await postgres.ExecuteNonQueryAsync(connectionString!, "DROP TABLE IF EXISTS " + table);
        }
    }

    [Fact]
    [Trait("Category", "LiveProvider")]
    public async Task WhereContains_CaseInsensitive_MatchesLikeTheEngineInMySql()
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_MYSQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DBACLIENTX_MYSQL_TEST_CONNECTION to a MySQL database.");
        var table = "dbax_contains_" + Guid.NewGuid().ToString("N");
        using var mySql = new DBAClientX.MySql();
        await mySql.ExecuteNonQueryAsync(connectionString!, "CREATE TABLE " + table + " (Id int PRIMARY KEY, Name varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL)");
        try
        {
            for (var i = 0; i < Values.Length; i++)
            {
                await mySql.ExecuteNonQueryAsync(connectionString!, "INSERT INTO " + table + " VALUES (@id, @name)", new Dictionary<string, object?> { ["@id"] = i, ["@name"] = Values[i] });
            }

            foreach (var needle in Needles)
            {
                var (text, parameters) = new Query().Select("Id").From(table).WhereContains("Name", needle, caseInsensitive: true).OrderBy("Id").CompileWithNamedParameters(SqlDialect.MySql);
                var actual = await mySql.QueryAsListAsync(connectionString!, text, record => record.GetInt32(0), parameters);
                var expected = Enumerable.Range(0, Values.Length)
                    .Where(i => Values[i].ToLowerInvariant().Contains(needle.ToLowerInvariant(), StringComparison.Ordinal))
                    .ToArray();
                Assert.True(expected.SequenceEqual(actual), $"Needle '{needle}': expected [{string.Join(",", expected)}], got [{string.Join(",", actual)}].");
            }
        }
        finally
        {
            await mySql.ExecuteNonQueryAsync(connectionString!, "DROP TABLE IF EXISTS " + table);
        }
    }
}