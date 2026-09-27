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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task KeysetReadPagesAsync_SQLiteMixedNumericKeys_RequireComparatorAndResume(bool descending)
    {
        using var database = new SharedMemoryDatabase();
        using var sqlite = new DBAClientX.SQLite();
        sqlite.ExecuteNonQueryWithConnectionString(database.ConnectionString,
            "CREATE TABLE mixed_keys (value); INSERT INTO mixed_keys VALUES (1), (1.5), (2), (2.5);");
        var column = descending ? KeysetColumn.Desc("value") : KeysetColumn.Asc("value");
        var source = new Query().Select("value").From("mixed_keys");
        IAsyncEnumerable<object> Execute(string sql, IDictionary<string, object?> parameters, CancellationToken ct)
            => sqlite.QueryStreamWithConnectionStringAsync(database.ConnectionString, sql, record => record.GetValue(0), parameters, cancellationToken: ct);
        var paging = new KeysetPagination(1, column);
        var yielded = 0;
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var page in paging.ReadPagesAsync(source, SqlDialect.SQLite, Execute, value => new object?[] { value })) yielded++;
        });
        Assert.Contains("mixed numeric", error.Message);
        Assert.Equal(0, yielded);

        // These fixture values fit exactly in decimal. Applications must choose a comparator for their own database domain.
        paging = new KeysetPagination(1, column)
        {
            CompareKeys = (left, right) => (descending ? -1 : 1) *
                Convert.ToDecimal(left[0], System.Globalization.CultureInfo.InvariantCulture).CompareTo(
                    Convert.ToDecimal(right[0], System.Globalization.CultureInfo.InvariantCulture)),
        };
        QueryPage<object>? first = null;
        await foreach (var page in paging.ReadPagesAsync(source, SqlDialect.SQLite, Execute, value => new object?[] { value }))
        {
            first = page;
            break;
        }
        Assert.NotNull(first!.NextCursor);
        var values = new List<object>(first.Items);
        await foreach (var value in paging.StreamAsync(source, SqlDialect.SQLite, Execute, value => new object?[] { value }, first.NextCursor)) values.Add(value);
        Assert.Equal(descending ? new object[] { 2.5d, 2L, 1.5d, 1L } : new object[] { 1L, 1.5d, 2L, 2.5d }, values);
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

    [Fact]
    public async Task KeysetStreamAsync_CancelledWithinPage_StopsBeforeYieldingAnotherRow()
    {
        var paging = new KeysetPagination(3, KeysetColumn.Asc<long>("id"));
        using var cancellation = new CancellationTokenSource();
        var yielded = 0;
        async IAsyncEnumerable<long> Rows()
        {
            await Task.Yield();
            yield return 1;
            yield return 2;
            yield return 3;
        }
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var row in paging.StreamAsync(new Query().From("t"), SqlDialect.SQLite, (_, _, _) => Rows(),
                row => new object?[] { row }, cancellationToken: cancellation.Token))
            {
                yielded++;
                cancellation.Cancel();
            }
        });
        Assert.Equal(1, yielded);
    }

    [Theory]
    [InlineData(false, 5L)]
    [InlineData(true, 15L)]
    [InlineData(false, 10L)]
    public async Task KeysetReadPagesAsync_RejectsBackwardAndEqualRowsBeforeYield(bool descending, long returned)
    {
        var paging = new KeysetPagination(2, descending ? KeysetColumn.Desc<long>("id") : KeysetColumn.Asc<long>("id"));
        var yielded = 0;
        var disposed = false;
        async IAsyncEnumerable<long> Rows()
        {
            try { await Task.Yield(); yield return returned; }
            finally { disposed = true; }
        }
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.SQLite,
                (_, _, _) => Rows(), row => new object?[] { row }, paging.CreateCursor(new object?[] { 10L }))) yielded++;
        });
        Assert.Equal(0, yielded);
        Assert.True(disposed);
    }

    [Fact]
    public async Task KeysetReadPagesAsync_CompositeKeys_RespectsEachDirection()
    {
        var paging = new KeysetPagination(2, KeysetColumn.Desc<long>("created"), KeysetColumn.Asc<long>("id"));
        async IAsyncEnumerable<long[]> Rows()
        {
            await Task.Yield();
            yield return new long[] { 10, 3 };
            yield return new long[] { 9, 1 };
        }
        var pages = new List<QueryPage<long[]>>();
        await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.SQLite, (_, _, _) => Rows(),
            row => new object?[] { row[0], row[1] }, paging.CreateCursor(new object?[] { 10L, 2L }))) pages.Add(page);
        Assert.Equal(2, Assert.Single(pages).Items.Count);
    }

    [Fact]
    public async Task KeysetReadPagesAsync_TextKeys_UsesConfiguredDatabaseCollation()
    {
        var paging = new KeysetPagination(2, KeysetColumn.Asc<string>("name"))
        {
            CompareKeys = (left, right) => StringComparer.OrdinalIgnoreCase.Compare((string)left[0]!, (string)right[0]!),
        };
        async IAsyncEnumerable<string> Rows()
        {
            await Task.Yield();
            yield return "B";
            yield return "c";
        }
        var pages = new List<QueryPage<string>>();
        await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.SQLite, (_, _, _) => Rows(),
            row => new object?[] { row }, paging.CreateCursor(new object?[] { "a" }))) pages.Add(page);
        Assert.Equal(new[] { "B", "c" }, Assert.Single(pages).Items);
    }
}
