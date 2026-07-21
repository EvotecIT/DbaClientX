using Azure;
using Azure.Data.Tables;

namespace DBAClientX.AzureTables;

/// <summary>Azure.Data.Tables implementation of the DbaClientX Azure Table boundary.</summary>
public sealed class AzureSdkTableStore : IDbaAzureTableStore
{
    private const int AzureTransactionLimit = 100;
    private readonly TableServiceClient _serviceClient;

    /// <summary>Creates a store from an Azure Tables service client.</summary>
    public AzureSdkTableStore(TableServiceClient serviceClient)
        => _serviceClient = serviceClient ?? throw new ArgumentNullException(nameof(serviceClient));

    /// <summary>Creates a store from an Azure Storage or Cosmos DB Table API connection string.</summary>
    public AzureSdkTableStore(string connectionString)
        : this(new TableServiceClient(connectionString))
    {
    }

    /// <summary>Normalized service endpoint used by this store.</summary>
    public Uri ServiceUri => _serviceClient.Uri;

    /// <inheritdoc />
    public async Task<DbaAzureTablePage> QueryPageAsync(
        string tableName,
        string? filter,
        IReadOnlyList<string>? select,
        string? continuationToken,
        int pageSize,
        CancellationToken cancellationToken = default)
    {
        ValidatePageSize(pageSize);
        var client = GetTableClient(tableName);
        await foreach (var page in client
                           .QueryAsync<TableEntity>(filter, pageSize, select, cancellationToken)
                           .AsPages(continuationToken, pageSize)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            return new DbaAzureTablePage(
                page.Values.Select(ToDbaEntity).ToArray(),
                page.ContinuationToken);
        }

        return new DbaAzureTablePage(Array.Empty<DbaAzureTableEntity>());
    }

    /// <inheritdoc />
    public async Task<long> CountAsync(string tableName, string? filter = null, CancellationToken cancellationToken = default)
    {
        long count = 0;
        var client = GetTableClient(tableName);
        await foreach (var page in client
                           .QueryAsync<TableEntity>(filter, maxPerPage: 1000, select: new[] { "PartitionKey", "RowKey" }, cancellationToken)
                           .AsPages(pageSizeHint: 1000)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            count = checked(count + page.Values.Count);
        }

        return count;
    }

    /// <inheritdoc />
    public async Task CreateTableIfNotExistsAsync(string tableName, CancellationToken cancellationToken = default)
        => await GetTableClient(tableName).CreateIfNotExistsAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    public async Task WriteAsync(
        string tableName,
        IReadOnlyList<DbaAzureTableEntity> entities,
        DbaAzureTableWriteMode mode,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (entities == null)
        {
            throw new ArgumentNullException(nameof(entities));
        }
        ValidateBatchSize(batchSize);
        var client = GetTableClient(tableName);

        foreach (var batch in DbaAzureTableBatchPlanner.Plan(entities, batchSize))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var actions = batch
                .Select(entity => new TableTransactionAction(ToActionType(mode), ToTableEntity(entity)))
                .ToArray();
            await client.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task ClearAsync(string tableName, int batchSize, CancellationToken cancellationToken = default)
    {
        ValidateBatchSize(batchSize);
        var client = GetTableClient(tableName);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await QueryFirstKeyPageAsync(client, cancellationToken).ConfigureAwait(false);
            if (page.Count == 0)
            {
                return;
            }

            foreach (var partition in page.GroupBy(static entity => entity.PartitionKey, StringComparer.Ordinal))
            {
                foreach (var batch in Batch(partition, batchSize))
                {
                    var actions = batch
                        .Select(entity => new TableTransactionAction(TableTransactionActionType.Delete, entity, ETag.All))
                        .ToArray();
                    await client.SubmitTransactionAsync(actions, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

    private TableClient GetTableClient(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null or whitespace.", nameof(tableName));
        }

        return _serviceClient.GetTableClient(tableName);
    }

    private static async Task<IReadOnlyList<TableEntity>> QueryFirstKeyPageAsync(
        TableClient client,
        CancellationToken cancellationToken)
    {
        await foreach (var page in client
                           .QueryAsync<TableEntity>(maxPerPage: AzureTransactionLimit, select: new[] { "PartitionKey", "RowKey" }, cancellationToken: cancellationToken)
                           .AsPages(pageSizeHint: AzureTransactionLimit)
                           .WithCancellation(cancellationToken)
                           .ConfigureAwait(false))
        {
            return page.Values;
        }

        return Array.Empty<TableEntity>();
    }

    private static DbaAzureTableEntity ToDbaEntity(TableEntity entity)
    {
        var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
        foreach (var property in entity)
        {
            if (!IsSystemProperty(property.Key))
            {
                properties[property.Key] = property.Value;
            }
        }

        return new DbaAzureTableEntity(
            entity.PartitionKey,
            entity.RowKey,
            properties,
            entity.Timestamp,
            entity.ETag.ToString());
    }

    private static TableEntity ToTableEntity(DbaAzureTableEntity entity)
    {
        var tableEntity = new TableEntity(entity.PartitionKey, entity.RowKey);
        foreach (var property in entity.Properties)
        {
            if (IsSystemProperty(property.Key))
            {
                continue;
            }

            tableEntity[property.Key] = NormalizePropertyValue(property.Key, property.Value);
        }

        return tableEntity;
    }

    private static object? NormalizePropertyValue(string propertyName, object? value)
        => value switch
        {
            null => null,
            DateTime dateTime => new DateTimeOffset(dateTime.ToUniversalTime(), TimeSpan.Zero),
            string or byte[] or bool or double or Guid or int or long or DateTimeOffset => value,
            _ => throw new ArgumentException(
                $"Azure Table property '{propertyName}' has unsupported CLR type '{value.GetType().FullName}'. " +
                "Supported values are string, byte[], bool, double, Guid, int, long, DateTime, DateTimeOffset, or null.")
        };

    private static bool IsSystemProperty(string name)
        => string.Equals(name, "PartitionKey", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(name, "RowKey", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(name, "Timestamp", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(name, "ETag", StringComparison.OrdinalIgnoreCase);

    private static TableTransactionActionType ToActionType(DbaAzureTableWriteMode mode)
        => mode switch
        {
            DbaAzureTableWriteMode.Add => TableTransactionActionType.Add,
            DbaAzureTableWriteMode.UpsertMerge => TableTransactionActionType.UpsertMerge,
            DbaAzureTableWriteMode.UpsertReplace => TableTransactionActionType.UpsertReplace,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unsupported Azure Table write mode.")
        };

    private static IEnumerable<IReadOnlyList<T>> Batch<T>(IEnumerable<T> source, int batchSize)
    {
        var batch = new List<T>(batchSize);
        foreach (var item in source)
        {
            batch.Add(item);
            if (batch.Count == batchSize)
            {
                yield return batch;
                batch = new List<T>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            yield return batch;
        }
    }

    private static void ValidatePageSize(int pageSize)
    {
        if (pageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be greater than zero.");
        }
    }

    private static void ValidateBatchSize(int batchSize)
    {
        if (batchSize is < 1 or > AzureTransactionLimit)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Azure Table transaction size must be between 1 and 100.");
        }
    }
}
