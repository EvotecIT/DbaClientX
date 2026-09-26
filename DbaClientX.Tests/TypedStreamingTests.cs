using System.Data;
using System.Runtime.CompilerServices;
using DBAClientX;
using DBAClientX.Mapping;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class TypedStreamingTests
{
    public enum EventStatus
    {
        Unknown = 0,
        Open = 1,
        Closed = 2,
    }

    public sealed class EventRow
    {
        public int Id { get; set; }
        public string? Name { get; set; } = "unset";
        public DateTime Created { get; set; }
        public EventStatus Status { get; set; }
        public EventStatus StatusName { get; set; }
        public Guid Token { get; set; }
        public decimal Amount { get; set; }
        public bool Flag { get; set; }
        public int? Optional { get; set; }
        public string NotInResult { get; set; } = "default";
        public int ReadOnly => 42;
    }

    [Fact]
    public async Task QueryStreamWithConnectionStringAsync_RecordMapper_ConvertsSQLiteValues()
    {
        var token = Guid.NewGuid();
        using var database = new SharedMemoryDatabase(
            "CREATE TABLE events (id INTEGER, name TEXT, created TEXT, status INTEGER, status_name TEXT, token TEXT, amount REAL, flag INTEGER, optional INTEGER, extra TEXT);" +
            $"INSERT INTO events VALUES (1, 'a', '2026-09-26T10:30:00.0000000Z', 2, 'open', '{token}', 12.5, 1, NULL, 'x');" +
            "INSERT INTO events VALUES (2, NULL, '2026-09-27T00:00:00', 0, 'CLOSED', '00000000-0000-0000-0000-000000000000', 3, 0, 7, NULL);");
        using var sqlite = new DBAClientX.SQLite();

        var rows = await ToListAsync(sqlite.QueryStreamWithConnectionStringAsync(
            database.ConnectionString,
            "SELECT id, name, created, status, status_name AS StatusName, token, amount, flag, optional, extra FROM events ORDER BY id",
            DbaRecordMapper.For<EventRow>()));

        Assert.Equal(2, rows.Count);
        var first = rows[0];
        Assert.Equal(1, first.Id);
        Assert.Equal("a", first.Name);
        Assert.Equal(new DateTime(2026, 9, 26, 10, 30, 0, DateTimeKind.Utc), first.Created);
        Assert.Equal(DateTimeKind.Utc, first.Created.Kind);
        Assert.Equal(EventStatus.Closed, first.Status);
        Assert.Equal(EventStatus.Open, first.StatusName);
        Assert.Equal(token, first.Token);
        Assert.Equal(12.5m, first.Amount);
        Assert.True(first.Flag);
        Assert.Null(first.Optional);
        Assert.Equal("default", first.NotInResult);
        var second = rows[1];
        Assert.Null(second.Name);
        Assert.Equal(EventStatus.Closed, second.StatusName);
        Assert.Equal(3m, second.Amount);
        Assert.False(second.Flag);
        Assert.Equal(7, second.Optional);
    }

    [Fact]
    public async Task RecordMapper_ReusedAcrossQueries_RebindsColumnOrdinals()
    {
        using var database = new SharedMemoryDatabase("CREATE TABLE t (id INTEGER, name TEXT); INSERT INTO t VALUES (1, 'a');");
        using var sqlite = new DBAClientX.SQLite();
        var map = DbaRecordMapper.For<EventRow>();

        var first = await ToListAsync(sqlite.QueryStreamWithConnectionStringAsync(database.ConnectionString, "SELECT id, name FROM t", map));
        var second = await ToListAsync(sqlite.QueryStreamWithConnectionStringAsync(database.ConnectionString, "SELECT name, id FROM t", map));

        Assert.Equal((1, "a"), (first[0].Id, first[0].Name));
        Assert.Equal((1, "a"), (second[0].Id, second[0].Name));
    }

    [Fact]
    public void RecordMapper_SameReaderWithNewSchema_Rebinds()
    {
        using var first = new DataTable();
        first.Columns.Add("id", typeof(long));
        first.Columns.Add("name", typeof(string));
        first.Rows.Add(1L, "a");
        using var second = new DataTable();
        second.Columns.Add("name", typeof(string));
        second.Columns.Add("id", typeof(long));
        second.Rows.Add("b", 2L);
        using var reader = new DataTableReader(new[] { first, second });
        var map = DbaRecordMapper.For<EventRow>();

        Assert.True(reader.Read());
        var a = map(reader);
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        var b = map(reader);

        Assert.Equal((1, "a"), (a.Id, a.Name));
        Assert.Equal((2, "b"), (b.Id, b.Name));
    }

    public sealed class TimeRow
    {
        public DateTimeOffset FromText { get; set; }
        public DateTimeOffset FromDateTime { get; set; }
        public DateTime FromOffset { get; set; }
        public TimeSpan Duration { get; set; }
        public Guid FromBytes { get; set; }
    }

    [Fact]
    public void RecordMapper_TimeAndGuidConversions_UseUtcForUnzonedValues()
    {
        var guid = Guid.NewGuid();
        using var table = new DataTable();
        table.Columns.Add("FromText", typeof(string));
        table.Columns.Add("FromDateTime", typeof(DateTime));
        table.Columns.Add("FromOffset", typeof(DateTimeOffset));
        table.Columns.Add("Duration", typeof(string));
        table.Columns.Add("FromBytes", typeof(byte[]));
        table.Rows.Add("2026-09-26 10:30:00", new DateTime(2026, 9, 26, 10, 30, 0, DateTimeKind.Unspecified), new DateTimeOffset(2026, 9, 26, 12, 30, 0, TimeSpan.FromHours(2)), "01:02:03", guid.ToByteArray());
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());

        var row = DbaRecordMapper.For<TimeRow>()(reader);

        var expected = new DateTimeOffset(2026, 9, 26, 10, 30, 0, TimeSpan.Zero);
        Assert.Equal(expected, row.FromText);
        Assert.Equal(TimeSpan.Zero, row.FromText.Offset);
        Assert.Equal(expected, row.FromDateTime);
        Assert.Equal(expected.UtcDateTime, row.FromOffset);
        Assert.Equal(DateTimeKind.Utc, row.FromOffset.Kind);
        Assert.Equal(new TimeSpan(1, 2, 3), row.Duration);
        Assert.Equal(guid, row.FromBytes);
    }

    [Fact]
    public async Task RecordMapper_IncompatibleValue_ThrowsWithColumnName()
    {
        using var database = new SharedMemoryDatabase("CREATE TABLE t (token TEXT); INSERT INTO t VALUES ('not-a-guid');");
        using var sqlite = new DBAClientX.SQLite();

        var exception = await Assert.ThrowsAnyAsync<Exception>(() => ToListAsync(
            sqlite.QueryStreamWithConnectionStringAsync(database.ConnectionString, "SELECT token FROM t", DbaRecordMapper.For<EventRow>())));

        var cast = Assert.IsType<InvalidCastException>(FindInnermost<InvalidCastException>(exception));
        Assert.Contains("'token'", cast.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task QueryStreamAsync_TypedMapper_ReadsOnlyConsumedRows()
    {
        using var sqlite = new DBAClientX.SQLite();
        var mapped = 0;
        var values = new List<long>();

        await foreach (var value in sqlite.QueryStreamAsync(
            ":memory:",
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 10000000) SELECT x FROM n",
            record =>
            {
                mapped++;
                return record.GetInt64(0);
            }))
        {
            values.Add(value);
            if (values.Count == 5)
            {
                break;
            }
        }

        Assert.Equal(new long[] { 1, 2, 3, 4, 5 }, values);
        Assert.Equal(5, mapped);
    }

    [Fact]
    public async Task ValuesMapper_ReplacesDbNullWithNull()
    {
        using var sqlite = new DBAClientX.SQLite();

        var rows = await ToListAsync(sqlite.QueryStreamAsync(":memory:", "SELECT 1 AS a, NULL AS b, 'x' AS c", DbaRecordMapper.Values()));

        Assert.Equal(new object?[] { 1L, null, "x" }, Assert.Single(rows));
    }

    [Fact]
    public async Task ChunkAsync_SplitsIntoBoundedChunks()
    {
        var chunks = await ToListAsync(Range(10).ChunkAsync(4));

        Assert.Equal(new[] { 4, 4, 2 }, chunks.Select(chunk => chunk.Count).ToArray());
        Assert.Equal(Enumerable.Range(0, 10), chunks.SelectMany(chunk => chunk));
        Assert.NotSame(chunks[0], chunks[1]);
        Assert.Empty(await ToListAsync(Range(0).ChunkAsync(4)));
        Assert.Throws<ArgumentOutOfRangeException>(() => Range(1).ChunkAsync(0));
    }

    [Fact]
    public async Task ChunkAsync_Cancelled_StopsEnumeration()
    {
        using var cancellation = new CancellationTokenSource();
        var seen = 0;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in Range(100).ChunkAsync(2, cancellation.Token))
            {
                seen++;
                cancellation.Cancel();
            }
        });

        Assert.Equal(1, seen);
    }

    private static async IAsyncEnumerable<int> Range(int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var index = 0; index < count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
            yield return index;
        }
    }

    private static async Task<List<T>> ToListAsync<T>(IAsyncEnumerable<T> source)
    {
        var list = new List<T>();
        await foreach (var item in source)
        {
            list.Add(item);
        }

        return list;
    }

    private static Exception? FindInnermost<T>(Exception exception)
        where T : Exception
    {
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            if (current is T)
            {
                return current;
            }
        }

        return null;
    }

    private sealed class SharedMemoryDatabase : IDisposable
    {
        private readonly SqliteConnection _keepAlive;

        public SharedMemoryDatabase(string setupSql)
        {
            ConnectionString = "Data Source=typed-stream-" + Guid.NewGuid().ToString("N") + ";Mode=Memory;Cache=Shared";
            _keepAlive = new SqliteConnection(ConnectionString);
            _keepAlive.Open();
            using var command = _keepAlive.CreateCommand();
            command.CommandText = setupSql;
            command.ExecuteNonQuery();
        }

        public string ConnectionString { get; }

        public void Dispose() => _keepAlive.Dispose();
    }
}
