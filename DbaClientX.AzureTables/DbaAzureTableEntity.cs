namespace DBAClientX.AzureTables;

/// <summary>Provider-neutral representation of one Azure Table entity.</summary>
public sealed class DbaAzureTableEntity
{
    /// <summary>Creates an entity from its required keys and optional properties.</summary>
    public DbaAzureTableEntity(
        string partitionKey,
        string rowKey,
        IReadOnlyDictionary<string, object?>? properties = null,
        DateTimeOffset? timestamp = null,
        string? etag = null)
    {
        if (partitionKey == null)
        {
            throw new ArgumentNullException(nameof(partitionKey));
        }

        if (rowKey == null)
        {
            throw new ArgumentNullException(nameof(rowKey));
        }

        PartitionKey = partitionKey;
        RowKey = rowKey;
        Properties = properties ?? new Dictionary<string, object?>(StringComparer.Ordinal);
        Timestamp = timestamp;
        ETag = etag;
    }

    /// <summary>Azure Table partition key.</summary>
    public string PartitionKey { get; }

    /// <summary>Azure Table row key.</summary>
    public string RowKey { get; }

    /// <summary>Entity properties excluding Azure system properties.</summary>
    public IReadOnlyDictionary<string, object?> Properties { get; }

    /// <summary>Server-maintained modification timestamp when returned by a query.</summary>
    public DateTimeOffset? Timestamp { get; }

    /// <summary>Entity tag used for optimistic concurrency when returned by a query.</summary>
    public string? ETag { get; }
}
