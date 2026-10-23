using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

[Collection(nameof(QueryCompilerCacheCollection))]
public sealed class SqlValueRankTests
{
    [Fact]
    public void Ordinal_CopiesTokensAndPreservesCaseWhitespaceAndQuotedValues()
    {
        var values = new[] { "A", "a", " A ", " ", "O'Reilly; --", "__proto__", "constructor", "\U00010000" };
        var expected = values.ToArray();
        var rank = SqlValueRank.Ordinal("q.state", values);
        values[0] = "changed";

        Assert.Equal("q.state", rank.Column);
        Assert.Equal(expected, rank.Tokens);
        Assert.Throws<NotSupportedException>(() => ((IList<string>)rank.Tokens)[0] = "changed again");
        var compiled = new Query().SelectValueRank(rank, "rank.alias\"quoted").From("items", "q").CompileWithParameters(SqlDialect.SQLite);
        Assert.Equal(expected.Cast<object>(), compiled.Parameters);
        Assert.Contains("AS \"rank.alias\"\"quoted\"", compiled.Sql);
        Assert.Contains("\"q\".\"state\" COLLATE BINARY", compiled.Sql);
        foreach (var token in expected.Where(value => value.Length > 2)) Assert.DoesNotContain(token, compiled.Sql);
    }

    [Fact]
    public void Ordinal_RejectsInvalidDeclarationsAndBoundsWithoutNormalizingTokens()
    {
        foreach (var column in new[] { "", " ", "*", "q.*", ".state", "q.", "q..state", "state\0" })
            Assert.Throws<ArgumentException>(() => SqlValueRank.Ordinal(column, new[] { "A" }));
        Assert.Throws<ArgumentNullException>(() => SqlValueRank.Ordinal("state", null!));
        foreach (var tokens in new[] { Array.Empty<string>(), new[] { "" }, new[] { (string)null! }, new[] { "A", "A" }, new[] { "\uD800" }, new[] { "\uDC00" }, new[] { new string('x', 4097) } })
            Assert.Throws<ArgumentException>(() => SqlValueRank.Ordinal("state", tokens));
        Assert.Throws<ArgumentException>(() => SqlValueRank.Ordinal("state", Enumerable.Repeat("x", 513).ToArray()));
        var oversized = Enumerable.Range(0, 257).Select(index => index.ToString("D3") + new string('x', 4093)).ToArray();
        Assert.Throws<ArgumentException>(() => SqlValueRank.Ordinal("state", oversized));
        var rank = SqlValueRank.Ordinal("state", new[] { "A" });
        Assert.Throws<ArgumentException>(() => new Query().SelectValueRank(rank, " "));
        Assert.Throws<ArgumentNullException>(() => new Query().OrderByValueRank(null!));
        Assert.Throws<ArgumentNullException>(() => KeysetColumn.ValueRank(null!, "rank"));
    }

    [Fact]
    public void Compile_BindsSelectScopeSeekAndOrderInTheirActualOccurrenceSequence()
    {
        var rank = SqlValueRank.Ordinal("state", new[] { "O'Reilly; --", " A " });
        var paging = Paging(rank);
        var source = Source(rank, "scope-one");
        var page = paging.CreatePageQuery(source, paging.CreateCursor(new object?[] { 0L, 10L }));
        var positional = page.CompileWithParameters(SqlDialect.SQLite);
        var named = page.CompileWithNamedParameters(SqlDialect.SQLite);
        var expected = SeekValues(rank.Tokens, "scope-one", 0L, 10L);

        Assert.Equal(expected, positional.Parameters);
        Assert.Equal(positional.Sql, named.Sql);
        Assert.Equal(expected.Length, named.Parameters.Count);
        for (var index = 0; index < expected.Length; index++) Assert.Equal(expected[index], named.Parameters["@p" + index]);
        foreach (var token in rank.Tokens) Assert.DoesNotContain(token, positional.Sql);
        Assert.StartsWith("SELECT \"id\", CASE", positional.Sql);
        Assert.Contains("ORDER BY CASE", positional.Sql);
        Assert.Empty(source.OrderByColumns);
        Assert.Null(source.LimitValue);
        Assert.Single(source.WhereTokens);
    }

    [Fact]
    public void Cache_RebindsChangedDeclarationsScopeAndCursorAcrossKeysetClones()
    {
        QueryCompiler.ClearCache();
        var firstRank = SqlValueRank.Ordinal("state", new[] { "first", "second" });
        var secondRank = SqlValueRank.Ordinal("state", new[] { "new first", "new second" });
        var firstPaging = Paging(firstRank);
        var secondPaging = Paging(secondRank);
        var firstQuery = firstPaging.CreatePageQuery(Source(firstRank, "scope-one"), firstPaging.CreateCursor(new object?[] { 0L, 3L }));
        var secondQuery = secondPaging.CreatePageQuery(Source(secondRank, "scope-two"), secondPaging.CreateCursor(new object?[] { 1L, 7L }));
        var first = firstQuery.CompileWithParameters(SqlDialect.SQLite);
        var cacheCount = QueryCompiler.CacheCount;
        var second = secondQuery.CompileWithParameters(SqlDialect.SQLite);

        Assert.Equal(first.Sql, second.Sql);
        Assert.Equal(cacheCount, QueryCompiler.CacheCount);
        Assert.Equal(SeekValues(firstRank.Tokens, "scope-one", 0L, 3L), first.Parameters);
        Assert.Equal(SeekValues(secondRank.Tokens, "scope-two", 1L, 7L), second.Parameters);
        var named = secondQuery.CompileWithNamedParameters(SqlDialect.SQLite);
        named.Parameters["@p0"] = "caller mutation";
        Assert.Equal("new first", secondQuery.CompileWithNamedParameters(SqlDialect.SQLite).Parameters["@p0"]);
        Assert.Equal("first", first.Parameters[0]);
    }

    [Fact]
    public void Cache_CollectsProjectionBeforeNestedSourceAndOrderAfterPredicatesInOffsetClones()
    {
        QueryCompiler.ClearCache();
        Query Build(string first, string second, int tenant, bool active)
        {
            var rank = SqlValueRank.Ordinal("q.state", new[] { first, second });
            return new Query().Select("q.id").SelectValueRank(rank, "rank")
                .From(new Query().Select("*").From("items").Where("tenant", tenant), "q")
                .Where("q.active", active).OrderByValueRank(rank).OrderBy("q.id");
        }
        var source = Build("one", "two", 11, true);
        var paging = new OffsetPagination(2);
        var first = paging.CreatePageQuery(source, 0).CompileWithParameters(SqlDialect.SQLite);
        var count = QueryCompiler.CacheCount;
        var second = paging.CreatePageQuery(Build("three", "four", 22, false), 0).CompileWithParameters(SqlDialect.SQLite);

        Assert.Equal(first.Sql, second.Sql);
        Assert.Equal(count, QueryCompiler.CacheCount);
        Assert.Equal(new object[] { "one", "two", 11, true, "one", "two" }, first.Parameters);
        Assert.Equal(new object[] { "three", "four", 22, false, "three", "four" }, second.Parameters);
        Assert.Null(source.LimitValue);
        Assert.Null(source.OffsetValue);
        Assert.Throws<InvalidOperationException>(() => paging.CreatePageQuery(source, 0).Compile(SqlDialect.SQLite));
    }

    [Fact]
    public void Cursor_FingerprintsOrderedTokenValuesColumnAndDirectionAndKeepsLegacyPlainKeys()
    {
        var rank = SqlValueRank.Ordinal("state", new[] { "act", "confirm" });
        var paging = Paging(rank);
        var cursor = paging.CreateCursor(new object?[] { 0L, 1L });
        var equivalent = Paging(SqlValueRank.Ordinal("state", new[] { "act", "confirm" }));
        Assert.NotNull(equivalent.CreatePageQuery(new Query().Select("id").From("items"), cursor));
        foreach (var changed in new[] {
            Paging(SqlValueRank.Ordinal("state", new[] { "confirm", "act" })),
            Paging(SqlValueRank.Ordinal("state", new[] { "act", "replacement" })),
            Paging(SqlValueRank.Ordinal("other_state", new[] { "act", "confirm" })),
            Paging(rank, descending: true)
        }) Assert.Throws<ArgumentException>(() => changed.CreatePageQuery(new Query().Select("id").From("items"), cursor));

        var key = KeysetColumn.ValueRank(rank, "rank");
        Assert.Equal(typeof(long), key.ValueType);
        Assert.Same(rank, key.Rank);
        Assert.Throws<InvalidOperationException>(() => paging.CreateCursor(new object?[] { 0d, 1L }));
        Assert.Throws<ArgumentException>(() => new KeysetPagination(2, key));
        Assert.Throws<InvalidOperationException>(() => key.WithCollation("NOCASE"));
        Assert.NotNull(paging.CreatePageQuery(Source(rank, "scope"), paging.CreateCursor(new object?[] { 0, 1 })));
        var plain = new KeysetPagination(10, new[] { KeysetColumn.Asc("name") }, KeysetColumn.Desc<long>("id"));
        Assert.Equal("dbax-page-v1.Aa59lE-ishZfvNHN6EOF7MUBAWECAQAAAAAAAAA", plain.CreateCursor(new object?[] { "a", 1L }));
    }

    [Fact]
    public void Cursor_HashesLargeDeclarationsWithoutEmbeddingTheirValuesInTheCursor()
    {
        var tokens = Enumerable.Range(0, 64).Select(index => index.ToString("D3") + new string('x', 4093)).ToArray();
        var paging = Paging(SqlValueRank.Ordinal("state", tokens));
        var cursor = paging.CreateCursor(new object?[] { 0L, 1L });
        Assert.InRange(cursor.Length, 1, 256);
        Assert.NotNull(paging.CreatePageQuery(new Query().Select("id").From("items"), cursor));
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer)]
    [InlineData(SqlDialect.PostgreSql)]
    [InlineData(SqlDialect.MySql)]
    [InlineData(SqlDialect.Oracle)]
    public void Compile_RejectsUnqualifiedDialectsWithoutChangingOrdinaryQueries(SqlDialect dialect)
    {
        var rank = SqlValueRank.Ordinal("state", new[] { "act" });
        Assert.Throws<NotSupportedException>(() => Source(rank, "scope").CompileWithParameters(dialect));
        Assert.Throws<NotSupportedException>(() => new Query().Select("id").From("items").OrderByValueRank(rank).CompileWithParameters(dialect));
        var ordinary = new Query().Select("id").From("items").Where("state", "act").CompileWithParameters(dialect);
        Assert.Equal(new object[] { "act" }, ordinary.Parameters);
        Assert.DoesNotContain("CASE", ordinary.Sql);
    }

    [Fact]
    public void Compile_RejectsLiteralRankPathsAndDirectCompoundOrdering()
    {
        var rank = SqlValueRank.Ordinal("state", new[] { "act" });
        Assert.Throws<InvalidOperationException>(() => Source(rank, "scope").Compile(SqlDialect.SQLite));
        Assert.Throws<InvalidOperationException>(() => new Query().Select("id").From("items").OrderByValueRank(rank).Compile(SqlDialect.SQLite));
        Assert.Throws<InvalidOperationException>(() => new Query().Select("id").From(Source(rank, "scope"), "q").Compile(SqlDialect.SQLite));
        Assert.Throws<InvalidOperationException>(() => Paging(rank).CreatePageQuery(new Query().Select("id").From("items")).Compile(SqlDialect.SQLite));
        var compound = Source(rank, "scope").UnionAll(Source(rank, "other")).OrderByValueRank(rank);
        Assert.Throws<NotSupportedException>(() => compound.CompileWithParameters(SqlDialect.SQLite));
    }

    private static KeysetPagination Paging(SqlValueRank rank, bool descending = false)
        => new(2, new[] { KeysetColumn.ValueRank(rank, "rank", descending) }, KeysetColumn.Asc<long>("id"));

    private static Query Source(SqlValueRank rank, object scope)
        => new Query().Select("id").SelectValueRank(rank, "rank").From("items").Where("tenant", scope);

    private static object[] SeekValues(IReadOnlyList<string> tokens, object scope, long cursorRank, long id)
        => tokens.Cast<object>().Concat(new[] { scope })
            .Concat(tokens.Cast<object>()).Concat(new object[] { cursorRank })
            .Concat(tokens.Cast<object>()).Concat(new object[] { cursorRank })
            .Concat(tokens.Cast<object>()).Concat(new object[] { cursorRank, id })
            .Concat(tokens.Cast<object>()).ToArray();
}
