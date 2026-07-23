using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

namespace DBAClientX.Diagnostics;

/// <summary>
/// Defines the dependency-free diagnostics contract shared by DbaClientX operations.
/// </summary>
public static class DbaClientXDiagnostics
{
    /// <summary>Name used by the DbaClientX <see cref="ActivitySource"/>.</summary>
    public const string ActivitySourceName = "DbaClientX";

    private static readonly ActivitySource SourceInstance = new(ActivitySourceName);
    private static readonly AsyncLocal<DbaOperationTelemetry?> CurrentTelemetry = new();

    /// <summary>Gets the activity source used by DbaClientX.</summary>
    public static ActivitySource ActivitySource => SourceInstance;

    /// <summary>
    /// Starts a correlated operation and establishes its operation identifier for nested work.
    /// </summary>
    /// <param name="activityName">Stable activity name.</param>
    /// <param name="requestedOperationId">Optional non-zero W3C trace identifier to continue.</param>
    /// <param name="tags">Optional redacted diagnostic tags.</param>
    /// <returns>A scope that restores the previous operation when disposed.</returns>
    public static DbaOperationScope StartOperation(
        string activityName,
        string? requestedOperationId,
        IEnumerable<KeyValuePair<string, object?>>? tags = null)
    {
        var currentActivity = Activity.Current;
        var normalizedRequestedId = NormalizeOperationId(requestedOperationId);
        if (currentActivity != null &&
            normalizedRequestedId != null &&
            !string.Equals(currentActivity.TraceId.ToString(), normalizedRequestedId, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "OperationId must match the active Activity trace identifier.",
                nameof(requestedOperationId));
        }

        Activity? activity;
        string operationId;
        if (currentActivity != null)
        {
            activity = SourceInstance.StartActivity(activityName, ActivityKind.Internal, currentActivity.Context, tags);
            operationId = currentActivity.TraceId.ToString();
        }
        else if (normalizedRequestedId != null)
        {
            var parent = new ActivityContext(
                ActivityTraceId.CreateFromString(normalizedRequestedId.AsSpan()),
                ActivitySpanId.CreateRandom(),
                ActivityTraceFlags.None,
                traceState: null,
                isRemote: true);
            activity = SourceInstance.StartActivity(activityName, ActivityKind.Internal, parent, tags);
            operationId = normalizedRequestedId;
        }
        else
        {
            activity = SourceInstance.StartActivity(activityName, ActivityKind.Internal, default(ActivityContext), tags);
            operationId = activity?.TraceId.ToString() ?? ActivityTraceId.CreateRandom().ToString();
        }

        var previousTelemetry = CurrentTelemetry.Value;
        var telemetry = new DbaOperationTelemetry();
        CurrentTelemetry.Value = telemetry;
        activity?.SetTag("dbaclientx.operation.id", operationId);
        return new DbaOperationScope(operationId, activity, telemetry, previousTelemetry);
    }

    /// <summary>Starts a child activity within the active operation.</summary>
    public static Activity? StartActivity(
        string activityName,
        IEnumerable<KeyValuePair<string, object?>>? tags = null)
    {
        var activity = SourceInstance.StartActivity(activityName, ActivityKind.Internal);
        if (activity != null && tags != null)
        {
            foreach (var tag in tags)
            {
                activity.SetTag(tag.Key, tag.Value);
            }
        }

        return activity;
    }

    internal static void RecordRetry(TransientRetryAttempt attempt)
        => RecordRetry(attempt.Attempt, attempt.Delay, attempt.Exception);

    /// <summary>Records a retry without exposing credentials or request payloads.</summary>
    public static void RecordRetry(int attempt, TimeSpan delay, Exception exception)
    {
        if (exception == null)
        {
            throw new ArgumentNullException(nameof(exception));
        }
        CurrentTelemetry.Value?.RecordRetry();
        Activity.Current?.AddEvent(new ActivityEvent(
            "dbaclientx.retry",
            tags: new ActivityTagsCollection
            {
                { "dbaclientx.retry.attempt", attempt },
                { "dbaclientx.retry.delay_ms", delay.TotalMilliseconds },
                { "error.type", exception.GetType().FullName }
            }));
    }

    /// <summary>Records a structured, redacted warning on the active operation.</summary>
    public static void RecordWarning(string code, string message)
    {
        var warning = new DbaDiagnosticWarning(code, message);
        CurrentTelemetry.Value?.RecordWarning(warning);
        Activity.Current?.AddEvent(new ActivityEvent(
            "dbaclientx.warning",
            tags: new ActivityTagsCollection
            {
                { "dbaclientx.warning.code", code },
                { "dbaclientx.warning.message", message }
            }));
    }

    /// <summary>Records only the exception type on an activity.</summary>
    public static void RecordException(Activity? activity, Exception exception)
    {
        if (activity == null)
        {
            return;
        }

        activity.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
        activity.AddEvent(new ActivityEvent(
            "exception",
            tags: new ActivityTagsCollection
            {
                { "exception.type", exception.GetType().FullName },
                { "exception.message", exception.GetType().Name }
            }));
    }

    internal static string SanitizeLogicalName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var trimmed = value!.Trim();
        var buffer = new char[Math.Min(trimmed.Length, 256)];
        var length = 0;
        foreach (var character in trimmed)
        {
            if (length == buffer.Length)
            {
                break;
            }

            buffer[length++] = char.IsControl(character) ? '_' : character;
        }

        return new string(buffer, 0, length);
    }

    private static string? NormalizeOperationId(string? operationId)
    {
        if (string.IsNullOrWhiteSpace(operationId))
        {
            return null;
        }

        var normalized = operationId!.Trim().ToLowerInvariant();
        if (normalized.Length != 32 ||
            normalized.All(static character => character == '0') ||
            normalized.Any(static character =>
                !char.IsDigit(character) &&
                (character < 'a' || character > 'f')))
        {
            throw new ArgumentException(
                "OperationId must be a non-zero 32-character W3C trace identifier.",
                nameof(operationId));
        }

        return normalized;
    }

    /// <summary>Summarizes retries and warnings observed during one operation.</summary>
    public sealed class DbaOperationTelemetry
    {
        private readonly ConcurrentQueue<DbaDiagnosticWarning> _warnings = new();
        private int _retryCount;

        /// <summary>Gets the number of retries recorded by the operation.</summary>
        public int RetryCount => _retryCount;

        /// <summary>Gets the structured warnings recorded by the operation.</summary>
        public IReadOnlyList<DbaDiagnosticWarning> Warnings => _warnings.ToArray();

        internal void RecordRetry()
            => Interlocked.Increment(ref _retryCount);

        internal void RecordWarning(DbaDiagnosticWarning warning)
            => _warnings.Enqueue(warning);
    }

    /// <summary>Represents a structured, redacted operation warning.</summary>
    public sealed record DbaDiagnosticWarning(string Code, string Message);

    /// <summary>Represents a correlated operation lifetime.</summary>
    public sealed class DbaOperationScope : IDisposable
    {
        private readonly DbaOperationTelemetry? _previousTelemetry;
        private bool _disposed;

        internal DbaOperationScope(
            string operationId,
            Activity? activity,
            DbaOperationTelemetry telemetry,
            DbaOperationTelemetry? previousTelemetry)
        {
            OperationId = operationId;
            Activity = activity;
            Telemetry = telemetry;
            _previousTelemetry = previousTelemetry;
        }

        /// <summary>Gets the stable W3C operation identifier.</summary>
        public string OperationId { get; }

        /// <summary>Gets the activity created for the operation when it is being observed.</summary>
        public Activity? Activity { get; }

        /// <summary>Gets the retry and warning summary for the operation.</summary>
        public DbaOperationTelemetry Telemetry { get; }

        /// <summary>Ends the activity and restores the previous operation context.</summary>
        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Activity?.Dispose();
            CurrentTelemetry.Value = _previousTelemetry;
        }
    }
}
