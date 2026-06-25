using System;
using System.Collections.Generic;

namespace DBAClientX.DataMovement;

/// <summary>
/// Describes one logical table-data copy operation.
/// </summary>
public sealed record DbaTableCopyDefinition(
    string SourceName,
    string DestinationName,
    IReadOnlyList<string>? OrderByColumns = null,
    string? LogicalName = null)
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
    }
}
