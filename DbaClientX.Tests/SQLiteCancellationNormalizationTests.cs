using System.Data;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class SQLiteCancellationNormalizationTests
{
    private sealed class ProviderFailureSQLite : DBAClientX.SQLite
    {
        public required CancellationTokenSource CancellationSource { get; init; }
        public required Exception Failure { get; init; }

        protected override Task<object?> ExecuteResolvedQueryAsync(
            SqliteConnection connection,
            SqliteTransaction? transaction,
            string query,
            IDictionary<string, object?>? parameters,
            CancellationToken cancellationToken,
            IDictionary<string, DbType>? parameterTypes,
            IDictionary<string, ParameterDirection>? parameterDirections)
        {
            CancellationSource.Cancel();
            return Task.FromException<object?>(Failure);
        }
    }

    [Fact]
    public async Task QueryWithConnectionStringAsync_WhenProviderReportsInterrupt_NormalizesCallerCancellation()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var cancellation = new CancellationTokenSource();
            var providerException = new SqliteException("interrupted", 9);
            using var sqlite = new ProviderFailureSQLite
            {
                CancellationSource = cancellation,
                Failure = providerException
            };

            var exception = await Assert.ThrowsAsync<OperationCanceledException>(() =>
                sqlite.QueryWithConnectionStringAsync(
                    $"Data Source={path};Pooling=False",
                    "SELECT 1",
                    cancellationToken: cancellation.Token));

            Assert.Equal(cancellation.Token, exception.CancellationToken);
            Assert.Same(providerException, exception.InnerException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task QueryAsListAsync_WhenMapperFailsAfterCancellation_PreservesQueryFailure()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            using var cancellation = new CancellationTokenSource();
            var mapperException = new InvalidOperationException("mapper failed");

            var exception = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
                sqlite.QueryAsListAsync<int>(
                    path,
                    "SELECT 1",
                    _ =>
                    {
                        cancellation.Cancel();
                        throw mapperException;
                    },
                    cancellationToken: cancellation.Token));

            Assert.Same(mapperException, exception.InnerException);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task QueryAsListAsync_WhenMapperThrowsUnrelatedCancellation_PreservesQueryFailure()
    {
        var path = Path.GetTempFileName();
        try
        {
            using var sqlite = new DBAClientX.SQLite();
            using var callerCancellation = new CancellationTokenSource();
            using var mapperCancellation = new CancellationTokenSource();
            mapperCancellation.Cancel();
            var mapperException = new OperationCanceledException(mapperCancellation.Token);

            var exception = await Assert.ThrowsAsync<DBAClientX.DbaQueryExecutionException>(() =>
                sqlite.QueryAsListAsync<int>(
                    path,
                    "SELECT 1",
                    _ =>
                    {
                        callerCancellation.Cancel();
                        throw mapperException;
                    },
                    cancellationToken: callerCancellation.Token));

            Assert.Same(mapperException, exception.InnerException);
            Assert.Equal(mapperCancellation.Token, mapperException.CancellationToken);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
