namespace DBAClientX.DataMovement;

/// <summary>
/// Describes a reusable multi-table copy plan generated from provider metadata.
/// </summary>
public sealed record DbaTableCopyPlan(
    IReadOnlyList<DbaTableCopyDefinition> Definitions,
    IReadOnlyList<DbaTableCopyPlanWarning> Warnings)
{
    /// <summary>Indicates whether the planner found any non-fatal issues callers may want to inspect.</summary>
    public bool HasWarnings => Warnings.Count > 0;
}

/// <summary>
/// Non-fatal issue discovered while generating a table-copy plan.
/// </summary>
public sealed record DbaTableCopyPlanWarning(
    string Code,
    string Message,
    string? TableName = null,
    string? ColumnName = null);
