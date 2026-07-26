namespace DBAClientX;

/// <summary>Contains SQL Server bulk-copy completion and correlation metadata.</summary>
public sealed class SqlServerBulkInsertResult
{
    /// <summary>Creates a bulk-copy result.</summary>
    public SqlServerBulkInsertResult(long rowsCopied, string operationId)
    {
        if (rowsCopied < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowsCopied));
        }

        if (string.IsNullOrWhiteSpace(operationId))
        {
            throw new ArgumentException("An operation identifier is required.", nameof(operationId));
        }

        RowsCopied = rowsCopied;
        OperationId = operationId;
    }

    /// <summary>Gets the number of rows reported by the provider bulk-copy operation.</summary>
    public long RowsCopied { get; }

    /// <summary>Gets the stable W3C operation identifier.</summary>
    public string OperationId { get; }
}
