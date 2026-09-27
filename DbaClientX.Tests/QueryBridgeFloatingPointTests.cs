using DBAClientX.QueryBuilder;
using Npgsql;

namespace DbaClientX.Tests;

public sealed class QueryBridgeFloatingPointTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task NaNKeys_RequireComparatorBeforeYieldingEvenWithOneRow(bool single, bool resume)
    {
        object nan = single ? (object)float.NaN : double.NaN;
        var paging = new KeysetPagination(1, KeysetColumn.Asc("value"));
        var yielded = 0;
        async IAsyncEnumerable<object> Rows()
        {
            await Task.Yield();
            yield return nan;
        }
        var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.PostgreSql,
                (_, _, _) => Rows(), value => new object?[] { value }, resume ? paging.CreateCursor(new object?[] { nan }) : null)) yielded++;
        });
        Assert.Contains("NaN", error.Message);
        Assert.Contains("CompareKeys", error.Message);
        Assert.Equal(0, yielded);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FloatingPointKeys_PostgreSqlOrderingWithComparator_ResumesAcrossNaNAndInfinities(bool descending)
    {
        var ordered = new[] { double.NegativeInfinity, -1d, 0d, double.PositiveInfinity, double.NaN };
        if (descending) Array.Reverse(ordered);
        var paging = new KeysetPagination(1, descending ? KeysetColumn.Desc<double>("value") : KeysetColumn.Asc<double>("value"))
        {
            CompareKeys = (left, right) => (descending ? -1 : 1) * ComparePostgreSqlDouble(left[0]!, right[0]!),
        };
        async IAsyncEnumerable<double> Execute(string _, IDictionary<string, object?> parameters, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Yield();
            foreach (var value in ordered)
                if (parameters.Count == 0 || paging.CompareKeys(new object?[] { value }, new object?[] { parameters.Values.First() }) > 0)
                    yield return value;
        }
        QueryPage<double>? first = null;
        await foreach (var page in paging.ReadPagesAsync(new Query().From("t"), SqlDialect.PostgreSql, Execute, value => new object?[] { value }))
        {
            first = page;
            break;
        }
        var actual = new List<double>(first!.Items);
        await foreach (var value in paging.StreamAsync(new Query().From("t"), SqlDialect.PostgreSql, Execute,
            value => new object?[] { value }, first.NextCursor)) actual.Add(value);
        Assert.Equal(ordered, actual);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [Trait("Category", "LiveProvider")]
    public async Task PostgreSqlFloatingPointKeys_LiveOrderingRequiresComparator(bool single, bool descending)
    {
        var connectionString = Environment.GetEnvironmentVariable("DBACLIENTX_POSTGRESQL_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString), "Set DBACLIENTX_POSTGRESQL_TEST_CONNECTION to an isolated PostgreSQL database.");
        var table = "dbax_keyset_" + Guid.NewGuid().ToString("N");
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        try
        {
            await using (var command = connection.CreateCommand())
            {
                command.CommandText = $"CREATE TABLE {table} (value {(single ? "real" : "double precision")} NOT NULL); INSERT INTO {table} VALUES ('-Infinity'), (-1), (0), ('Infinity'), ('NaN');";
                await command.ExecuteNonQueryAsync();
            }
            using var postgres = new DBAClientX.PostgreSql();
            IAsyncEnumerable<object> Execute(string sql, IDictionary<string, object?> parameters, CancellationToken ct)
                => postgres.QueryStreamAsync(connectionString!, sql, record => record.GetValue(0), parameters, cancellationToken: ct);
            var source = new Query().Select("value").From(table);
            var column = descending ? KeysetColumn.Desc("value") : KeysetColumn.Asc("value");
            var paging = new KeysetPagination(1, column);
            var error = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var _ in paging.StreamAsync(source, SqlDialect.PostgreSql, Execute, value => new object?[] { value })) { }
            });
            Assert.Contains("NaN", error.Message);
            paging = new KeysetPagination(1, column)
            {
                CompareKeys = (left, right) => (descending ? -1 : 1) * ComparePostgreSqlDouble(left[0]!, right[0]!),
            };
            QueryPage<object>? first = null;
            await foreach (var page in paging.ReadPagesAsync(source, SqlDialect.PostgreSql, Execute, value => new object?[] { value }))
            {
                first = page;
                break;
            }
            var actual = new List<object>(first!.Items);
            await foreach (var value in paging.StreamAsync(source, SqlDialect.PostgreSql, Execute, value => new object?[] { value }, first.NextCursor)) actual.Add(value);
            var expected = new[] { double.NegativeInfinity, -1d, 0d, double.PositiveInfinity, double.NaN };
            if (descending) Array.Reverse(expected);
            Assert.Equal(expected, actual.Select(value => Convert.ToDouble(value)));
            Assert.All(actual, value => Assert.Equal(single ? typeof(float) : typeof(double), value.GetType()));
        }
        finally
        {
            await using var command = connection.CreateCommand();
            command.CommandText = $"DROP TABLE IF EXISTS {table}";
            await command.ExecuteNonQueryAsync();
        }
    }

    private static int ComparePostgreSqlDouble(object left, object right)
    {
        var first = Convert.ToDouble(left);
        var second = Convert.ToDouble(right);
        return double.IsNaN(first) ? (double.IsNaN(second) ? 0 : 1) : double.IsNaN(second) ? -1 : first.CompareTo(second);
    }
}
