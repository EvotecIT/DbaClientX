namespace DBAClientX.AzureTables;

/// <summary>Narrow Azure Table boundary used by the reusable adapter and offline contract tests.</summary>
public interface IDbaAzureTableStore
{
    /// <summary>Reads one page without interpreting the provider continuation token.</summary>
    Task<DbaAzureTablePage> QueryPageAsync(
        string tableName,
        string? filter,
        IReadOnlyList<string>? select,
        string? continuationToken,
        int pageSize,
        CancellationToken cancellationToken = default);

    /// <summary>Counts entities matching an optional filter.</summary>
    Task<long> CountAsync(string tableName, string? filter = null, CancellationToken cancellationToken = default);

    /// <summary>Creates the table when it does not exist.</summary>
    Task CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>Writes entities using partition-safe transactions.</summary>
    Task WriteAsync(
        string tableName,
        IReadOnlyList<DbaAzureTableEntity> entities,
        DbaAzureTableWriteMode mode,
        int batchSize,
        CancellationToken cancellationToken = default);

    /// <summary>Deletes all entities without deleting the table resource.</summary>
    Task ClearAsync(string tableName, int batchSize, CancellationToken cancellationToken = default);
}
