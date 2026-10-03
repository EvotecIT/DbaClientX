using System.Diagnostics;
using DBAClientX;
using DBAClientX.Diagnostics;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

[Collection("ExecutionDiagnostics")]
public sealed class NonReplayableCommandTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task NativeCommand_InvokesOnceDespiteReplayOptIns(bool observe, bool synchronousFailure)
    {
        int stopped = 0;
        using var listener = observe ? new ActivityListener
        {
            ShouldListenTo = source => source.Name == DbaClientXDiagnostics.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStopped = _ => stopped++
        } : null;
        if (listener != null) ActivitySource.AddActivityListener(listener);
        using var connection = new SqliteConnection("Data Source=:memory:;Pooling=False");
        await connection.OpenAsync();
        using var client = new Client
        {
            CommandRetryMode = CommandRetryMode.ReplaySafe,
            RetryNonQueryOperations = true,
            MaxRetryAttempts = 3
        };
        int calls = 0;
        var failure = new SqliteException("Provider failure.", 5);
        Task<int> Attempt()
        {
            calls++;
            if (synchronousFailure) throw failure;
            return Task.FromException<int>(failure);
        }

        var actual = await Assert.ThrowsAsync<SqliteException>(() => client.InvokeOnce(Attempt, connection));
        Assert.Same(failure, actual);
        Assert.Equal(1, calls);
        Assert.Equal(observe ? 1 : 0, stopped);
    }

    [Fact]
    public async Task NativeCommand_PreservesCallerCancellationAndDoesNotInvokeWhenAlreadyCanceled()
    {
        using var connection = new SqliteConnection("Data Source=:memory:;Pooling=False");
        await connection.OpenAsync();
        using var client = new Client();
        using var cancellation = new CancellationTokenSource();
        int calls = 0;
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.InvokeOnce(() =>
        {
            calls++;
            cancellation.Cancel();
            return Task.FromException<int>(new OperationCanceledException());
        }, connection, cancellation.Token));
        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Equal(1, calls);

        exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => client.InvokeOnce(() =>
        {
            calls++;
            return Task.FromResult(1);
        }, connection, cancellation.Token));
        Assert.Equal(cancellation.Token, exception.CancellationToken);
        Assert.Equal(1, calls);
    }

    private sealed class Client : SQLite
    {
        internal Task<int> InvokeOnce(Func<Task<int>> operation, SqliteConnection connection, CancellationToken token = default)
            => ExecuteNonReplayableCommandAsync(operation, connection, "native administrative command", token);
    }
}
