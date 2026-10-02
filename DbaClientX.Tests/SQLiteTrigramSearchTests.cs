using DBAClientX;
using DBAClientX.QueryBuilder;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public sealed class SQLiteTrigramSearchTests : IDisposable
{
    // Hostile names: every SQLite quote character and the option-string quote.
    private const string Table = "Ho\"st's [x]";
    private const string Key = "I\"d";
    private const string Name = "Na'me\"";
    private const string Site = "Si]te`";
    private const string Index = "Ho\"st's search";

    private static readonly string[] Names = { "dc0001.warsaw.corp", "DC0002.Warsaw.corp", "a_lab", "a%lab", "NEAR(x) AND y", "say \"hi\"", "Zażółć gęślą", "ZAŻÓŁĆ", "ab", "" };

    private readonly string _database = Path.Combine(Path.GetTempPath(), "dbx-trigram-" + Guid.NewGuid().ToString("N") + ".db");
    private readonly SQLite _sqlite = new();

    public SQLiteTrigramSearchTests()
    {
        _sqlite.ExecuteNonQuery(_database, $"CREATE TABLE {Q(Table)} ({Q(Key)} INTEGER PRIMARY KEY, {Q(Name)} TEXT, {Q(Site)} TEXT)");
        for (var i = 0; i < Names.Length; i++)
        {
            Insert(i, Names[i], i % 2 == 0 ? "Berlin" : null);
        }
    }

    [Fact]
    public async Task MatchingKeys_FindsTheRowsThatContainTheTextIgnoringCase()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name, Site });

        foreach (var needle in new[] { "warsaw", "WARSAW", "_la", "%la", "NEAR(", "\"hi\"", "żół", "ŻÓŁĆ", "erl", "nothing" })
        {
            var expected = Enumerable.Range(0, Names.Length)
                .Where(i => Names[i].ToLowerInvariant().Contains(needle.ToLowerInvariant(), StringComparison.Ordinal) ||
                            (i % 2 == 0 && "berlin".Contains(needle.ToLowerInvariant(), StringComparison.Ordinal)))
                .Select(i => (long)i)
                .ToArray();

            Assert.Equal(expected, await KeysAsync(needle));
        }
    }

    [Fact]
    public async Task MatchingKeys_WithAColumn_SearchesOnlyThatColumn()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name, Site });

        Assert.Equal(new long[] { 0, 2, 4, 6, 8 }, await KeysAsync("erl", Site));
        Assert.Empty(await KeysAsync("erl", Name));
        Assert.Equal(new long[] { 0, 1 }, await KeysAsync("warsaw", Name));
    }

    [Fact]
    public async Task MatchingKeys_WithSeveralColumnsAndTexts_FindsAnyTextInThoseColumns()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name, Site });
        var texts = new[] { "erl", "warsaw", "\"hi\"" };

        Assert.Equal(new long[] { 0, 1, 5 }, await KeysAsync(SQLiteTrigramSearch.MatchingKeys(Index, texts, Name)));
        Assert.Equal(new long[] { 0, 2, 4, 6, 8 }, await KeysAsync(SQLiteTrigramSearch.MatchingKeys(Index, texts, Site)));
        Assert.Equal(new long[] { 0, 1, 2, 4, 5, 6, 8 }, await KeysAsync(SQLiteTrigramSearch.MatchingKeys(Index, texts, Site, Name)));
        Assert.Equal(new long[] { 0, 1, 2, 4, 5, 6, 8 }, await KeysAsync(SQLiteTrigramSearch.MatchingKeys(Index, texts)));
        Assert.Equal(new long[] { 0, 1 }, await KeysAsync(SQLiteTrigramSearch.MatchingKeys(Index, new[] { "warsaw" }, Site, Name)));
    }

    [Fact]
    public async Task Triggers_KeepTheIndexCurrentOnInsertUpdateAndDelete()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name, Site });

        Insert(100, "new.lagos.host", null);
        _sqlite.ExecuteNonQuery(_database, $"UPDATE {Q(Table)} SET {Q(Name)} = 'moved.lima.host' WHERE {Q(Key)} = 0");
        _sqlite.ExecuteNonQuery(_database, $"DELETE FROM {Q(Table)} WHERE {Q(Key)} = 1");

        _sqlite.ExecuteNonQuery(_database, $"UPDATE {Q(Table)} SET {Q(Key)} = 300 WHERE {Q(Key)} = 2");

        Assert.Equal(new long[] { 100 }, await KeysAsync("lagos"));
        Assert.Equal(new long[] { 0 }, await KeysAsync("lima"));
        Assert.Empty(await KeysAsync("warsaw"));
        Assert.Equal(new long[] { 300 }, await KeysAsync("a_lab"));
        AssertIndexIntact();
    }

    [Fact]
    public async Task InsertOrReplace_WithoutRecursiveTriggers_LeavesSearchesCorrect()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name });
        _sqlite.ExecuteNonQuery(_database, $"UPDATE {Q(Table)} SET {Q(Site)} = 'unique-' || {Q(Key)}");
        _sqlite.ExecuteNonQuery(_database, $"CREATE UNIQUE INDEX ux ON {Q(Table)} ({Q(Site)})");

        // Replaces row 0 by key and row 3 through the unique Site; neither fires the delete trigger.
        _sqlite.ExecuteNonQuery(_database, $"INSERT OR REPLACE INTO {Q(Table)} VALUES (0, 'fresh.oslo.host', 'unique-0')");
        _sqlite.ExecuteNonQuery(_database, $"INSERT OR REPLACE INTO {Q(Table)} VALUES (50, 'other.oslo.host', 'unique-3')");

        Assert.Empty(await KeysAsync("dc0001"));
        Assert.Empty(await KeysAsync("a%lab"));
        Assert.Equal(new long[] { 0, 50 }, await KeysAsync("oslo"));
        AssertIndexIntact();
    }

    [Fact]
    public async Task Create_RejectsAKeyThatIsNotTheRowId()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Coded (Code TEXT PRIMARY KEY, Name TEXT)");
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE NoRowId (Id INTEGER PRIMARY KEY, Name TEXT) WITHOUT ROWID");

        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.CreateTrigramIndexAsync(_database, "CodedSearch", "Coded", "Code", new[] { "Name" }));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.CreateTrigramIndexAsync(_database, "NoRowIdSearch", "NoRowId", "Id", new[] { "Name" }));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Name, new[] { Site }));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.CreateTrigramIndexAsync(_database, "MissingSearch", "Missing", "Id", new[] { "Name" }));
        await _sqlite.CreateTrigramIndexAsync(_database, "CodedSearch", "Coded", "rowid", new[] { "Name" });
    }

    [Fact]
    public async Task Drop_RefusesATableThatIsNotAnIndex()
    {
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.DropTrigramIndexAsync(_database, Table));

        Assert.Equal((long)Names.Length, Convert.ToInt64(_sqlite.ExecuteScalar(_database, $"SELECT count(*) FROM {Q(Table)}")));
        await _sqlite.DropTrigramIndexAsync(_database, "NoSuchIndex");
    }

    [Fact]
    public async Task UpdatesThroughAnyRowIdNameAndGeneratedColumns_KeepTheIndexCurrent()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Gen (Id INTEGER PRIMARY KEY, Raw TEXT, Shown TEXT GENERATED ALWAYS AS ('host ' || Raw) VIRTUAL)");
        _sqlite.ExecuteNonQuery(_database, "INSERT INTO Gen (Id, Raw) VALUES (1, 'warsaw'), (2, 'berlin')");
        await _sqlite.CreateTrigramIndexAsync(_database, "GenSearch", "Gen", "rowid", new[] { "Shown" });

        _sqlite.ExecuteNonQuery(_database, "UPDATE Gen SET Raw = 'lima' WHERE Id = 1");
        _sqlite.ExecuteNonQuery(_database, "UPDATE Gen SET Id = 300 WHERE Id = 2");
        _sqlite.ExecuteNonQuery(_database, "UPDATE Gen SET oid = 400 WHERE Id = 300");

        async Task<long[]> Find(string text)
        {
            var (sql, parameters) = new Query().Select("Id").From("Gen").WhereInRaw("Id", SQLiteTrigramSearch.MatchingKeys("GenSearch", text)).OrderBy("Id").CompileWithNamedParameters(SqlDialect.SQLite);
            return (await _sqlite.QueryReadOnlyAsListAsync(_database, sql, reader => reader.GetInt64(0), parameters)).ToArray();
        }

        Assert.Equal(new long[] { 1 }, await Find("lima"));
        Assert.Empty(await Find("warsaw"));
        Assert.Equal(new long[] { 400 }, await Find("berlin"));
    }

    [Fact]
    public async Task Create_RejectsADescendingIntegerKeyAndShadowedRowIdNames()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Desc1 (Id INTEGER PRIMARY KEY DESC, Name TEXT)");
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Shadow (Id INTEGER PRIMARY KEY, rowid TEXT, Name TEXT)");

        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.CreateTrigramIndexAsync(_database, "DescSearch", "Desc1", "Id", new[] { "Name" }));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.CreateTrigramIndexAsync(_database, "ShadowSearch", "Shadow", "Id", new[] { "Name" }));
    }

    [Fact]
    public async Task Drop_RefusesADataTableNamedInAnotherCase()
    {
        _sqlite.ExecuteNonQuery(_database, "CREATE TABLE Plain (Id INTEGER PRIMARY KEY)");

        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.DropTrigramIndexAsync(_database, "PLAIN"));
        await Assert.ThrowsAsync<ArgumentException>(() => _sqlite.RebuildTrigramIndexAsync(_database, "plain", Table, Key, new[] { Name }));
        Assert.Equal(1L, Convert.ToInt64(_sqlite.ExecuteScalar(_database, "SELECT count(*) FROM sqlite_master WHERE name = 'Plain'")));
    }

    [Fact]
    public async Task Rebuild_AfterAColumnRename_IndexesTheRenamedColumn()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name });
        _sqlite.ExecuteNonQuery(_database, $"ALTER TABLE {Q(Table)} RENAME COLUMN {Q(Name)} TO Renamed");

        await _sqlite.RebuildTrigramIndexAsync(_database, Index, Table, Key, new[] { "Renamed" });
        Insert(203, "renamed.cairo.host", null);

        Assert.Equal(new long[] { 0, 1 }, await KeysAsync("warsaw"));
        Assert.Equal(new long[] { 203 }, await KeysAsync("cairo"));
        AssertIndexIntact();
    }

    private void AssertIndexIntact()
        => _sqlite.ExecuteNonQuery(_database, $"INSERT INTO {Q(Index)}({Q(Index)}, rank) VALUES ('integrity-check', 1)");

    [Fact]
    public async Task Rebuild_RefillsAndDrop_RemovesTheIndexAndTriggers()
    {
        await _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name });
        _sqlite.ExecuteNonQuery(_database, $"DROP TRIGGER {Q(Index + "_ai")}");
        Insert(200, "late.sydney.host", null);
        Assert.Empty(await KeysAsync("sydney"));

        await _sqlite.RebuildTrigramIndexAsync(_database, Index, Table, Key, new[] { Name });
        Assert.Equal(new long[] { 200 }, await KeysAsync("sydney"));
        Insert(202, "after.rebuild.host", null);
        Assert.Equal(new long[] { 202 }, await KeysAsync("after.reb"));

        await _sqlite.DropTrigramIndexAsync(_database, Index);
        Assert.Equal(0L, Convert.ToInt64(_sqlite.ExecuteScalar(_database, "SELECT count(*) FROM sqlite_master WHERE name LIKE 'Ho\"st''s search%'")));
        Insert(201, "still.writable", null);
    }

    [Fact]
    public async Task Create_WhenItFails_LeavesNoPartialIndex()
    {
        await Assert.ThrowsAsync<DbaQueryExecutionException>(() => _sqlite.CreateTrigramIndexAsync(_database, Index, Table, Key, new[] { Name, "Missing" }));

        Assert.Equal(0L, Convert.ToInt64(_sqlite.ExecuteScalar(_database, "SELECT count(*) FROM sqlite_master WHERE name LIKE 'Ho\"st''s search%'")));
    }

    [Theory]
    [InlineData("ab", false)]
    [InlineData("abc", true)]
    [InlineData("a😀", false)]
    [InlineData("a😀b", true)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void CanMatch_CountsUnicodeCharacters(string? text, bool expected)
        => Assert.Equal(expected, SQLiteTrigramSearch.CanMatch(text));

    [Fact]
    public void MatchingKeys_QuotesTheTextAsAPhraseParameter()
    {
        var (sql, parameters) = SQLiteTrigramSearch.MatchingKeys("idx", "a\" OR b", "co\"l").CompileWithParameters(SqlDialect.SQLite);

        Assert.Equal("SELECT rowid FROM \"idx\" WHERE \"idx\" MATCH @p0", sql);
        Assert.Equal(new object[] { "\"co\"\"l\" : \"a\"\" OR b\"" }, parameters);
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", "ab"));
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", "abc\0"));
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", "abc", string.Empty));

        var (several, severalParameters) = SQLiteTrigramSearch.MatchingKeys("idx", new[] { "abc", "x\"y}z" }, "a", "b}\" c").CompileWithParameters(SqlDialect.SQLite);
        Assert.Equal("SELECT rowid FROM \"idx\" WHERE \"idx\" MATCH @p0", several);
        Assert.Equal(new object[] { "{\"a\" \"b}\"\" c\"} : (\"abc\" OR \"x\"\"y}z\")" }, severalParameters);
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", Array.Empty<string>()));
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", new[] { "abc", "ab" }));
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", "abc", "a", "A"));
        Assert.Throws<ArgumentException>(() => SQLiteTrigramSearch.MatchingKeys("idx", "abc", "a", null!));
    }

    private Task<long[]> KeysAsync(string needle, params string[] columns) => KeysAsync(SQLiteTrigramSearch.MatchingKeys(Index, needle, columns));

    private async Task<long[]> KeysAsync(Query keys)
    {
        var (sql, parameters) = new Query().SelectRaw(Q(Key)).FromRaw(Q(Table))
            .WhereInRaw(Q(Key), keys)
            .OrderByRaw(Q(Key))
            .CompileWithNamedParameters(SqlDialect.SQLite);
        return (await _sqlite.QueryReadOnlyAsListAsync(_database, sql, reader => reader.GetInt64(0), parameters)).ToArray();
    }

    private void Insert(int id, string name, string? site)
        => _sqlite.ExecuteNonQuery(_database, $"INSERT INTO {Q(Table)} VALUES (@id, @name, @site)", new Dictionary<string, object?> { ["@id"] = id, ["@name"] = name, ["@site"] = site });

    private static string Q(string name) => SqlIdentifier.Quote(SqlDialect.SQLite, name);

    public void Dispose()
    {
        _sqlite.Dispose();
        SqliteConnection.ClearAllPools();
        try
        {
            File.Delete(_database);
        }
        catch (IOException)
        {
        }
    }
}
