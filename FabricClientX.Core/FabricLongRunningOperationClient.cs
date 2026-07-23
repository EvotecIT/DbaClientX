using DBAClientX.Diagnostics;

namespace FabricClientX;

/// <summary>Polls Microsoft Fabric long-running operations and retrieves their results.</summary>
public sealed class FabricLongRunningOperationClient
{
    private readonly FabricHttpClient _transport;
    private readonly Func<TimeSpan, CancellationToken, Task> _delayAsync;

    /// <summary>Creates an operation client over a caller-configured transport.</summary>
    public FabricLongRunningOperationClient(
        FabricHttpClient transport,
        Func<TimeSpan, CancellationToken, Task>? delayAsync = null)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _delayAsync = delayAsync ??
            ((delay, cancellationToken) => Task.Delay(delay, cancellationToken));
    }

    /// <summary>Waits for a Fabric operation and retrieves its typed result.</summary>
    public async Task<FabricOperationResult<T>> WaitForCompletionAsync<T>(
        Guid serviceOperationId,
        TimeSpan timeout,
        TimeSpan? defaultPollInterval = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        if (serviceOperationId == Guid.Empty)
        {
            throw new ArgumentException("A service operation identifier is required.", nameof(serviceOperationId));
        }

        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        var pollInterval = defaultPollInterval ?? TimeSpan.FromSeconds(5);
        if (pollInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(defaultPollInterval));
        }

        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.LongRunningOperation",
            operationId);
        using var deadlineCancellation =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadlineCancellation.CancelAfter(timeout);
        var deadlineToken = deadlineCancellation.Token;
        var startedAt = DateTimeOffset.UtcNow;
        try
        {
            while (true)
            {
                deadlineToken.ThrowIfCancellationRequested();
                var stateResponse = await _transport.GetAsync<FabricOperationState>(
                    $"operations/{serviceOperationId:D}",
                    operation.OperationId,
                    deadlineToken).ConfigureAwait(false);
                var state = stateResponse.Value ??
                    throw new InvalidOperationException("The service returned an empty operation state.");

                if (string.Equals(state.Status, "Succeeded", StringComparison.OrdinalIgnoreCase))
                {
                    var resultUri = stateResponse.Location ??
                        new Uri($"operations/{serviceOperationId:D}/result", UriKind.Relative);
                    var result = await _transport.GetAsync<T>(
                        resultUri.ToString(),
                        operation.OperationId,
                        deadlineToken).ConfigureAwait(false);
                    return new FabricOperationResult<T>(
                        state,
                        result.Value,
                        operation.OperationId,
                        serviceOperationId);
                }

                if (string.Equals(state.Status, "Failed", StringComparison.OrdinalIgnoreCase))
                {
                    throw new FabricApiException(
                        System.Net.HttpStatusCode.InternalServerError,
                        state.Error?.ErrorCode ?? "FabricOperationFailed",
                        state.Error?.RequestId);
                }

                var remaining = timeout - (DateTimeOffset.UtcNow - startedAt);
                if (remaining <= TimeSpan.Zero)
                {
                    throw new TimeoutException(
                        $"Fabric operation {serviceOperationId:D} did not settle within {timeout}.");
                }

                var delay = stateResponse.RetryAfter ?? pollInterval;
                delay = delay < TimeSpan.Zero ? TimeSpan.Zero : delay;
                await _delayAsync(
                        delay > remaining ? remaining : delay,
                        deadlineToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (
            !cancellationToken.IsCancellationRequested &&
            deadlineCancellation.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"Fabric operation {serviceOperationId:D} did not settle within {timeout}.");
        }
    }
}
