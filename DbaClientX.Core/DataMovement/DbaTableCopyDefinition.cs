using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.DataMovement;

/// <summary>
/// Describes one logical table-data copy operation.
/// </summary>
public sealed record DbaTableCopyDefinition(
    string SourceName,
    string DestinationName,
    IReadOnlyList<string>? OrderByColumns = null,
    string? LogicalName = null,
    IReadOnlyDictionary<string, string>? ColumnMappings = null,
    IReadOnlyCollection<string>? ExcludedColumns = null,
    IReadOnlyDictionary<string, DbaTableCopyColumnType>? ColumnTypeConversions = null,
    DbaTableCopySourceOptions? SourceOptions = null)
{
    /// <summary>Human-friendly name used in progress and result output.</summary>
    public string DisplayName => string.IsNullOrWhiteSpace(LogicalName) ? DestinationName : LogicalName!;

    /// <summary>Validates required copy definition fields.</summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(SourceName))
        {
            throw new ArgumentException("Source name cannot be null or whitespace.", nameof(SourceName));
        }

        if (string.IsNullOrWhiteSpace(DestinationName))
        {
            throw new ArgumentException("Destination name cannot be null or whitespace.", nameof(DestinationName));
        }

        ValidateNames(ColumnMappings?.Keys, "Column mapping source column names cannot be null or whitespace.");
        ValidateNames(ColumnMappings?.Values, "Column mapping destination column names cannot be null or whitespace.");
        ValidateNames(ExcludedColumns, "Excluded column names cannot be null or whitespace.");
        ValidateNames(ColumnTypeConversions?.Keys, "Column type conversion column names cannot be null or whitespace.");
        ValidateNames(SourceOptions?.DeduplicateByColumns, "Source deduplication column names cannot be null or whitespace.");
        ValidateNames(SourceOptions?.DeduplicateOrderByColumns, "Source deduplication order column names cannot be null or whitespace.");
    }

    private static void ValidateNames(IEnumerable<string>? names, string message)
    {
        if (names == null)
        {
            return;
        }

        if (names.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException(message);
        }
    }
}
