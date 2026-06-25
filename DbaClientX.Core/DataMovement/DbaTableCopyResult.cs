using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.DataMovement;

/// <summary>
/// Summarizes a multi-table data copy operation.
/// </summary>
public sealed record DbaTableCopyResult(IReadOnlyList<DbaTableCopyTableResult> Tables, TimeSpan Duration)
{
    /// <summary>Total source rows counted before copy when known.</summary>
    public long? SourceRows => SumKnown(static table => table.SourceRows, Tables);

    /// <summary>Total rows written during copy.</summary>
    public long CopiedRows => Tables.Sum(static table => table.CopiedRows);

    /// <summary>Total destination rows counted after copy when known.</summary>
    public long? DestinationRows => SumKnown(static table => table.DestinationRows, Tables);

    /// <summary>Indicates whether every table with known source and destination counts matched.</summary>
    public bool Verified => Tables.All(static table => table.Verified);

    private static long? SumKnown(Func<DbaTableCopyTableResult, long?> selector, IReadOnlyList<DbaTableCopyTableResult> tables)
    {
        var total = 0L;
        foreach (var table in tables)
        {
            var value = selector(table);
            if (!value.HasValue)
            {
                return null;
            }

            total += value.Value;
        }

        return total;
    }
}

/// <summary>
/// Summarizes one table-data copy operation.
/// </summary>
public sealed record DbaTableCopyTableResult(
    string TableName,
    long? SourceRows,
    long CopiedRows,
    long? DestinationRows,
    bool Verified);
