using System.Data;
using DBAClientX.Mapping;
using DBAClientX.QueryBuilder;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class QueryBridgeHelpersTests
{
    public sealed class EventRow
    {
        public long Id { get; set; }
        public long Created { get; set; }
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer, "@p")]
    [InlineData(SqlDialect.PostgreSql, "@p")]
    [InlineData(SqlDialect.MySql, "@p")]
    [InlineData(SqlDialect.SQLite, "@p")]
    [InlineData(SqlDialect.Oracle, ":p")]
    public void CompileWithNamedParameters_KeysMatchCompiledPlaceholders(SqlDialect dialect, string prefix)
    {
        var query = new Query().From("t").Where("a", 1).Where("b", "x");

        var (sql, parameters) = query.CompileWithNamedParameters(dialect);

        Assert.Equal(new[] { prefix + "0", prefix + "1" }, parameters.Keys.ToArray());
        Assert.Equal(new object?[] { 1, "x" }, parameters.Values.ToArray());
        Assert.All(parameters.Keys, key => Assert.Contains(key, sql, StringComparison.Ordinal));
        Assert.Equal(parameters, QueryParameters.ToDictionary(query.CompileWithParameters(dialect).Parameters, dialect));
    }

    [Fact]
    public async Task KeysetStreamAsync_SQLite_StreamsEveryRowAcrossPages()
    {
        using var database = new SharedMemoryDatabase();
        using var sqlite = new DBAClientX.SQLite();
        var paging = new KeysetPagination(3, KeysetColumn.Desc<long>("created"), KeysetColumn.Asc<long>("id"));
        var source = new Query().Select("id", "created").From("events").Where("zone", "<>", "ap");
        var pageQueries = 0;

        var ids = new List<long>();
        await foreach (var row in paging.StreamAsync(
            source,
            SqlDialect.SQLite,
            (sql, parameters, ct) =>
            {
                pageQueries++;
                return sqlite.QueryStreamWithConnectionStringAsync(database.ConnectionString, sql, DbaRecordMapper.For<EventRow>(), parameters, cancellationToken: ct);
            },
            row => new object?[] { row.Created, row.Id }))
        {
            ids.Add(row.Id);
        }

        Assert.Equal(new long[] { 5, 6, 7, 3, 4, 1, 2 }, ids);
        Assert.Equal(3, pageQueries);
    }

    [Fact]
    public async Task KeysetReadPagesAsync_StopAndResume_ContinuesAfterCursor()
    {
        using var database = new SharedMemoryDatabase();
        using var sqlite = new DBAClientX.SQLite();
        var paging = new KeysetPagination(2, KeysetColumn.Asc<long>("id"));
        var source = new Query().Select("id", "created").From("events");
        IAsyncEnumerable<EventRow> Execute(string sql, IDictionary<string, object?> parameters, CancellationToken ct)
            => sqlite.QueryStreamWithConnectionStringAsync(database.ConnectionString, sql, DbaRecordMapper.For<EventRow>(), parameters, cancellationToken: ct);

        QueryPage<EventRow>? first = null;
        await foreach (var page in paging.ReadPagesAsync(source, SqlDialect.SQLite, Execute, row => new object?[] { row.Id }))
        {
            first = page;
            break;
        }

        var rest = new List<long>();
        await foreach (var row in paging.StreamAsync(source, SqlDialect.SQLite, Execute, row => new object?[] { row.Id }, first!.NextCursor))
        {
            rest.Add(row.Id);
        }

        Assert.Equal(new long[] { 1, 2 }, first.Items.Select(row => row.Id).ToArray());
        Assert.Equal(new long[] { 3, 4, 5, 6, 7, 8 }, rest);
    }

    [Fact]
    public async Task KeysetReadPagesAsync_PageDoesNotAdvance_Throws()
    {
        var paging = new KeysetPagination(1, KeysetColumn.Asc("created"));

        static async IAsyncEnumerable<long> SameRows()
        {
            await Task.Yield();
            yield return 10;
            yield return 10;
        }

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in paging.StreamAsync(new Query().From("t"), SqlDialect.SQLite, (_, _, _) => SameRows(), row => new object?[] { row }))
            {
            }
        });
    }

    [Fact]
    public async Task KeysetReadPagesAsync_StopsReadingAfterExtraRowAndDisposesSource()
    {
        var paging = new KeysetPagination(2, KeysetColumn.Asc<long>("id"));
        var pulled = 0;
        var disposed = 0;
        var calls = 0;

        async IAsyncEnumerable<long> Rows(long start)
        {
            try
            {
                for (var value = start; value < start + 100; value++)
                {
                    await Task.Yield();
                    pulled++;
                    yield return value;
                }
            }
            finally
            {
                disposed++;
            }
        }

        var pages = new List<QueryPage<long>>();
        await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.SQLite, (_, parameters, _) => Rows(calls++ * 2 + 1), row => new object?[] { row }))
        {
            pages.Add(page);
            break;
        }

        Assert.Equal(new long[] { 1, 2 }, pages[0].Items);
        Assert.NotNull(pages[0].NextCursor);
        Assert.Equal(3, pulled);
        Assert.Equal(1, disposed);
        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task KeysetReadPagesAsync_EmptyResult_YieldsOneEmptyPage()
    {
        var paging = new KeysetPagination(2, KeysetColumn.Asc<long>("id"));

        static async IAsyncEnumerable<long> None()
        {
            await Task.Yield();
            yield break;
        }

        var pages = new List<QueryPage<long>>();
        await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.SQLite, (_, _, _) => None(), row => new object?[] { row }))
        {
            pages.Add(page);
        }

        var single = Assert.Single(pages);
        Assert.Empty(single.Items);
        Assert.Null(single.NextCursor);
    }

    [Fact]
    public async Task KeysetStreamAsync_CancelledBetweenPages_StopsBeforeNextQuery()
    {
        var paging = new KeysetPagination(1, KeysetColumn.Asc<long>("id"));
        using var cancellation = new CancellationTokenSource();
        var calls = 0;

        async IAsyncEnumerable<long> Rows(long start)
        {
            await Task.Yield();
            yield return start;
            yield return start + 1;
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in paging.StreamAsync(new Query().From("t"), SqlDialect.SQLite, (_, _, _) => Rows(++calls), row => new object?[] { row }, cancellationToken: cancellation.Token))
            {
                cancellation.Cancel();
            }
        });

        Assert.Equal(1, calls);
        Assert.Throws<ArgumentNullException>(() => paging.StreamAsync<long>(new Query().From("t"), SqlDialect.SQLite, null!, row => new object?[] { row }));
    }

    private sealed class SharedMemoryDatabase : IDisposable
    {
        private readonly SqliteConnection _keepAlive;

        public SharedMemoryDatabase()
        {
            ConnectionString = "Data Source=bridge-" + Guid.NewGuid().ToString("N") + ";Mode=Memory;Cache=Shared";
            _keepAlive = new SqliteConnection(ConnectionString);
            _keepAlive.Open();
            using var command = _keepAlive.CreateCommand();
            command.CommandText =
                "CREATE TABLE events (id INTEGER PRIMARY KEY, created INTEGER NOT NULL, zone TEXT NOT NULL);" +
                "INSERT INTO events VALUES (1, 10, 'eu'), (2, 10, 'us'), (3, 20, 'eu'), (4, 20, 'eu'), (5, 30, 'us'), (6, 30, 'eu'), (7, 30, 'eu'), (8, 40, 'ap');";
            command.ExecuteNonQuery();
        }

        public string ConnectionString { get; }

        public void Dispose() => _keepAlive.Dispose();
    }
}
