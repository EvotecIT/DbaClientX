using DBAClientX.Diagnostics;

namespace FabricClientX.PowerBI;

/// <summary>Provides typed Power BI semantic-model discovery and refresh workflows.</summary>
public sealed class PowerBiClient
{
    /// <summary>The Microsoft Entra scope normally used for Power BI REST access.</summary>
    public const string DefaultScope = "https://analysis.windows.net/powerbi/api/.default";

    /// <summary>The global Power BI REST endpoint.</summary>
    public static readonly Uri DefaultBaseAddress = new("https://api.powerbi.com/v1.0/myorg/");

    private readonly FabricHttpClient _transport;
    private readonly Func<TimeSpan, CancellationToken, Task> _delayAsync;

    /// <summary>Creates a Power BI client over a caller-configured transport.</summary>
    public PowerBiClient(
        FabricHttpClient transport,
        Func<TimeSpan, CancellationToken, Task>? delayAsync = null)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
        _delayAsync = delayAsync ??
            ((delay, cancellationToken) => Task.Delay(delay, cancellationToken));
    }

    /// <summary>Lists semantic models in a Power BI workspace.</summary>
    public Task<FabricCollectionResult<PowerBiSemanticModel>> ListSemanticModelsAsync(
        Guid workspaceId,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentifier(workspaceId, nameof(workspaceId));
        return _transport.GetAllPagesResultAsync<PowerBiSemanticModel>(
            $"groups/{workspaceId:D}/datasets",
            operationId,
            cancellationToken);
    }

    /// <summary>Requests a semantic-model refresh without retrying the non-idempotent POST.</summary>
    public async Task<PowerBiRefreshStartResult> StartRefreshAsync(
        Guid workspaceId,
        Guid semanticModelId,
        PowerBiRefreshRequest? request = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ValidateIdentifier(workspaceId, nameof(workspaceId));
        ValidateIdentifier(semanticModelId, nameof(semanticModelId));
        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.PowerBI.Refresh",
            operationId);
        var response = await _transport.PostAsync(
            $"groups/{workspaceId:D}/datasets/{semanticModelId:D}/refreshes",
            request ?? new PowerBiRefreshRequest(),
            operation.OperationId,
            cancellationToken).ConfigureAwait(false);

        if (response.Location == null)
        {
            throw new InvalidOperationException(
                "Power BI accepted the refresh without returning its Location header.");
        }

        if (!TryResolveRefreshId(response.RequestId, response.Location, out var refreshId))
        {
            throw new InvalidOperationException(
                "Power BI accepted the refresh without returning a valid refresh identifier.");
        }

        return new PowerBiRefreshStartResult(
            workspaceId,
            semanticModelId,
            refreshId,
            response.Location,
            operation.OperationId);
    }

    /// <summary>Waits for an accepted Power BI refresh to reach a terminal state.</summary>
    public async Task<PowerBiRefreshSettlement> WaitForRefreshAsync(
        PowerBiRefreshStartResult start,
        TimeSpan timeout,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
    {
        if (start == null)
        {
            throw new ArgumentNullException(nameof(start));
        }

        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(timeout));
        }

        var interval = pollInterval ?? TimeSpan.FromSeconds(10);
        if (interval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(pollInterval));
        }

        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.PowerBI.Refresh",
            start.OperationId);
        var startedAt = DateTimeOffset.UtcNow;
        while (DateTimeOffset.UtcNow - startedAt <= timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var response = await _transport.GetAsync<PowerBiRefreshDetail>(
                start.Location.ToString(),
                operation.OperationId,
                cancellationToken).ConfigureAwait(false);
            var detail = response.Value ??
                throw new InvalidOperationException("Power BI returned an empty refresh status.");

            if (IsTerminal(detail.Status))
            {
                return new PowerBiRefreshSettlement(start, detail);
            }

            if (!IsInProgress(detail.Status))
            {
                throw new InvalidOperationException(
                    "Power BI returned an unsupported refresh status.");
            }

            await _delayAsync(
                response.RetryAfter ?? interval,
                cancellationToken).ConfigureAwait(false);
        }

        throw new TimeoutException(
            $"Power BI refresh {start.RefreshId:D} did not settle within {timeout}.");
    }

    /// <summary>Requests and waits for a semantic-model refresh.</summary>
    public async Task<PowerBiRefreshSettlement> RefreshAndWaitAsync(
        Guid workspaceId,
        Guid semanticModelId,
        PowerBiRefreshRequest? request,
        TimeSpan timeout,
        TimeSpan? pollInterval = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var start = await StartRefreshAsync(
            workspaceId,
            semanticModelId,
            request,
            operationId,
            cancellationToken).ConfigureAwait(false);
        return await WaitForRefreshAsync(
            start,
            timeout,
            pollInterval,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Cancels an accepted refresh. The caller must explicitly invoke this destructive operation.</summary>
    public Task<FabricResponse> CancelRefreshAsync(
        PowerBiRefreshStartResult start,
        CancellationToken cancellationToken = default)
    {
        if (start == null)
        {
            throw new ArgumentNullException(nameof(start));
        }

        return _transport.DeleteAsync(
            start.Location.ToString(),
            start.OperationId,
            cancellationToken);
    }

    private static bool IsTerminal(string status)
        => string.Equals(status, "Completed", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, "Failed", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, "Cancelled", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, "Disabled", StringComparison.OrdinalIgnoreCase);

    private static bool IsInProgress(string status)
        => string.Equals(status, "Unknown", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, "NotStarted", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, "Running", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(status, "InProgress", StringComparison.OrdinalIgnoreCase);

    private static bool TryResolveRefreshId(string? requestId, Uri location, out Guid refreshId)
    {
        if (Guid.TryParse(requestId, out refreshId))
        {
            return true;
        }

        var lastSegment = location.Segments.LastOrDefault()?.Trim('/');
        return Guid.TryParse(lastSegment, out refreshId);
    }

    private static void ValidateIdentifier(Guid value, string parameterName)
    {
        if (value == Guid.Empty)
        {
            throw new ArgumentException("A non-empty identifier is required.", parameterName);
        }
    }
}
