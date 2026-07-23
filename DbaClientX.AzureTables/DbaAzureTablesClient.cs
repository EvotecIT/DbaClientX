namespace DBAClientX.AzureTables;

/// <summary>Thin reusable client for querying and mutating Azure Table data.</summary>
public sealed class DbaAzureTablesClient
{
    private readonly IDbaAzureTableStore _store;

    /// <summary>Creates a client over a store implementation.</summary>
    public DbaAzureTablesClient(IDbaAzureTableStore store)
        => _store = store ?? throw new ArgumentNullException(nameof(store));

    /// <summary>Creates a client from an Azure Storage or Cosmos DB Table API connection string.</summary>
    public DbaAzureTablesClient(string connectionString)
        : this(new AzureSdkTableStore(connectionString))
    {
    }

    /// <summary>Reads one entity page while preserving the Azure continuation token.</summary>
    public Task<DbaAzureTablePage> QueryPageAsync(
        string tableName,
        string? filter = null,
        IReadOnlyList<string>? select = null,
        string? continuationToken = null,
        int pageSize = 1000,
        CancellationToken cancellationToken = default)
        => _store.QueryPageAsync(tableName, filter, select, continuationToken, pageSize, cancellationToken);

    /// <summary>Counts entities matching an optional filter.</summary>
    public Task<long> CountAsync(string tableName, string? filter = null, CancellationToken cancellationToken = default)
        => _store.CountAsync(tableName, filter, cancellationToken);

    /// <summary>Creates a table when it does not exist.</summary>
    public Task CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
        => _store.CreateTableIfNotExistsAsync(tableName, cancellationToken);

    /// <summary>Writes entities in partition-safe transactions.</summary>
    public async Task WriteAsync(
        string tableName,
        IReadOnlyList<DbaAzureTableEntity> entities,
        DbaAzureTableWriteMode mode = DbaAzureTableWriteMode.UpsertReplace,
        int batchSize = 100,
        bool createTable = true,
        CancellationToken cancellationToken = default)
    {
        if (entities == null)
        {
            throw new ArgumentNullException(nameof(entities));
        }
        DbaAzureTableDataMapper.ValidateEntities(entities);
        _ = DbaAzureTableBatchPlanner.Plan(entities, batchSize);
        if (createTable)
        {
            await _store.CreateTableIfNotExistsAsync(tableName, cancellationToken).ConfigureAwait(false);
        }

        await _store.WriteAsync(tableName, entities, mode, batchSize, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Deletes every entity while preserving the table resource.</summary>
    public Task ClearAsync(string tableName, int batchSize = 100, CancellationToken cancellationToken = default)
        => _store.ClearAsync(tableName, batchSize, cancellationToken);
}
