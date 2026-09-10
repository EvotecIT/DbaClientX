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
    /// <summary>
    /// Uses the last ordered key instead of an offset for SQLite and SQL Server reads.
    /// OrderByColumns must identify a unique, non-null key in the source, in ascending order.
    /// </summary>
    public bool UseKeysetPagination { get; init; }

    /// <summary>
    /// Optional unique, non-null destination key for content-verification reads, using destination column names.
    /// Use this when a provider generates a different key or its string collation does not preserve source uniqueness.
    /// When omitted, verification uses the mapped source order columns, which must be copied unchanged.
    /// </summary>
    public IReadOnlyList<string>? DestinationOrderByColumns { get; init; }

    /// <summary>Human-friendly name used in progress and result output.</summary>
    public string DisplayName => string.IsNullOrWhiteSpace(LogicalName) ? DestinationName : LogicalName!;

    /// <summary>Validates required copy definition fields.</summary>
    public void Validate()
    {
        if (UseKeysetPagination && (OrderByColumns == null || OrderByColumns.Count == 0 || OrderByColumns.Any(string.IsNullOrWhiteSpace)))
        {
            throw new ArgumentException("Keyset pagination requires explicit unique, non-null order columns.", nameof(OrderByColumns));
        }
        if (DestinationOrderByColumns != null && (DestinationOrderByColumns.Count == 0 || DestinationOrderByColumns.Any(string.IsNullOrWhiteSpace)))
            throw new ArgumentException("Destination verification requires explicit unique, non-null order columns.", nameof(DestinationOrderByColumns));
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
        ValidateUniqueNames(ColumnMappings?.Values, "Column mapping destination column names cannot contain duplicates.");
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

    private static void ValidateUniqueNames(IEnumerable<string>? names, string message)
    {
        if (names == null)
        {
            return;
        }

        var duplicate = names
            .Where(static name => !string.IsNullOrWhiteSpace(name))
            .GroupBy(static name => name, StringComparer.Ordinal)
            .FirstOrDefault(static group => group.Skip(1).Any());
        if (duplicate != null)
        {
            throw new ArgumentException(message);
        }
    }
}
