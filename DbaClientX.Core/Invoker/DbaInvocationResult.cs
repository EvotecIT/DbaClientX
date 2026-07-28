namespace DBAClientX.Invoker;

/// <summary>
/// Describes a completed provider invocation.
/// </summary>
/// <param name="Provider">Canonical provider name.</param>
/// <param name="Kind">Operation kind.</param>
/// <param name="CompletedExecutions">Number of item or payload executions that completed.</param>
/// <param name="AffectedRows">Total affected rows reported by the provider.</param>
public sealed record DbaInvocationResult(
    string Provider,
    DbaInvocationKind Kind,
    int CompletedExecutions,
    int AffectedRows);
