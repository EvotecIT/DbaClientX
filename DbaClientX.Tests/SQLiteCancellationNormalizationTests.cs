using System.Data;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class SQLiteCancellationNormalizationTests
{
    private sealed class StatefulCancellationSQLite : DBAClientX.SQLite
    {
        public Task<int> RunAsync(CancellationTokenSource source, Exception failure, bool synchronous)
            => AwaitWithCallerCancellationAsync(static (state, token) => {
                if (token != state.Source.Token) throw new InvalidOperationException("Caller token was not supplied.");
                state.Source.Cancel();
                if (state.Synchronous) throw state.Failure;
                return Task.FromException<int>(state.Failure);
            }, (Source: source, Failure: failure, Synchronous: synchronous), source.Token);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StatefulProviderAwait_RecognizedInterrupt_NormalizesSynchronousAndTaskFailures(bool synchronous)
    {
        using var cancellation = new CancellationTokenSource();
        using var sqlite = new StatefulCancellationSQLite();
        var failure = new SqliteException("interrupted", 9);
        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() => sqlite.RunAsync(cancellation, failure, synchronous));
        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.IsType<DBAClientX.DbaClientXException>(exception.InnerException);
        Assert.NotSame(failure, exception.InnerException);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StatefulProviderAwait_UnrelatedFailureAfterCancellation_PreservesFailure(bool synchronous)
    {
        using var cancellation = new CancellationTokenSource();
        using var sqlite = new StatefulCancellationSQLite();
        var failure = new InvalidOperationException("ordinary provider failure");
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => sqlite.RunAsync(cancellation, failure, synchronous));
        Assert.Same(failure, exception);
    }

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
            Assert.IsType<DBAClientX.DbaClientXException>(exception.InnerException);
            Assert.NotSame(providerException, exception.InnerException);
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

            Assert.IsType<InvalidOperationException>(exception.InnerException);
            Assert.NotSame(mapperException, exception.InnerException);
            Assert.Equal(typeof(InvalidOperationException).FullName, exception.ProviderExceptionType);
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

            var sanitized = Assert.IsType<OperationCanceledException>(exception.InnerException);
            Assert.NotSame(mapperException, sanitized);
            Assert.Equal(mapperCancellation.Token, sanitized.CancellationToken);
            Assert.Equal(typeof(OperationCanceledException).FullName, exception.ProviderExceptionType);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
