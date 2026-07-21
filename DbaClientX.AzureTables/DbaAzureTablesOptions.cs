namespace DBAClientX.AzureTables;

/// <summary>Controls how Azure Table names are compared for destructive copy-safety checks.</summary>
public enum DbaAzureTableNameComparison
{
    /// <summary>Use case-sensitive comparison for Cosmos DB Table API and case-insensitive comparison for Azure Storage Tables.</summary>
    Auto,
    /// <summary>Compare table names case-sensitively.</summary>
    Ordinal,
    /// <summary>Compare table names case-insensitively.</summary>
    OrdinalIgnoreCase
}

/// <summary>Configures Azure Tables query and copy behavior.</summary>
public sealed class DbaAzureTablesOptions
{
    /// <summary>Optional OData filter applied to source reads and source counts.</summary>
    public string? SourceFilter { get; init; }

    /// <summary>Optional source projection. PartitionKey and RowKey are always included.</summary>
    public IReadOnlyList<string>? SelectColumns { get; init; }

    /// <summary>Write behavior used for destination entities.</summary>
    public DbaAzureTableWriteMode WriteMode { get; init; } = DbaAzureTableWriteMode.UpsertReplace;

    /// <summary>Create a missing destination table before the first write.</summary>
    public bool CreateDestinationTable { get; init; } = true;

    /// <summary>Allow explicit ClearDestination operations.</summary>
    public bool AllowClearDestination { get; init; }

    /// <summary>
    /// Enumerate entities for source and destination row-count verification. Disable for large tables when the extra scans are undesirable.
    /// </summary>
    public bool EnableRowCounts { get; init; } = true;

    /// <summary>Default transaction size. Azure Table transactions are capped at 100 entities in one partition.</summary>
    public int BatchSize { get; init; } = 100;

    /// <summary>Table-name comparison used by destructive copy-safety checks.</summary>
    public DbaAzureTableNameComparison TableNameComparison { get; init; } = DbaAzureTableNameComparison.Auto;

    internal void Validate()
    {
        if (BatchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(BatchSize), "Azure Table batch size must be between 1 and 100.");
        }
        if (!Enum.IsDefined(typeof(DbaAzureTableNameComparison), TableNameComparison))
        {
            throw new ArgumentOutOfRangeException(nameof(TableNameComparison), "Unsupported Azure Table name comparison mode.");
        }
    }
}
