namespace DBAClientX.AzureTables;

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

    internal void Validate()
    {
        if (BatchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(BatchSize), "Azure Table batch size must be between 1 and 100.");
        }
    }
}
