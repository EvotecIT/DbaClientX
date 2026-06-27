namespace DBAClientX.DataMovement;

/// <summary>
/// Describes a source page request for a table copy.
/// </summary>
public sealed record DbaTableCopyPageRequest(DbaTableCopyDefinition Definition, long Offset, int PageSize);
