namespace DBAClientX.DataMovement;

/// <summary>
/// Reports progress for one table-data copy operation.
/// </summary>
public sealed record DbaTableCopyProgress(
    string TableName,
    long RowsCopied,
    long? SourceRows,
    int PageRows)
{
    /// <summary>Copy percentage when source row count is known.</summary>
    public double? PercentComplete
        => SourceRows is > 0
            ? RowsCopied >= SourceRows.Value ? 100d : RowsCopied * 100d / SourceRows.Value
            : null;
}
