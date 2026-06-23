using System.Collections.Generic;

namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server table copy and sync command plan generated from metadata.
/// </summary>
public sealed class SqlServerTableCopyPlan
{
    /// <summary>Source schema name.</summary>
    public string SourceSchema { get; set; } = string.Empty;

    /// <summary>Source table name.</summary>
    public string SourceTable { get; set; } = string.Empty;

    /// <summary>Destination schema name.</summary>
    public string DestinationSchema { get; set; } = string.Empty;

    /// <summary>Destination table name.</summary>
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Column mappings included in the plan.</summary>
    public IReadOnlyList<SqlServerTableCopyColumnInfo> Columns { get; set; } = [];

    /// <summary>Inferred key column names, normally from the primary key.</summary>
    public IReadOnlyList<string> KeyColumns { get; set; } = [];

    /// <summary>Command that reads rows from the source table.</summary>
    public string SourceSelectCommand { get; set; } = string.Empty;

    /// <summary>Parameterized command that inserts one destination row.</summary>
    public string DestinationInsertCommand { get; set; } = string.Empty;

    /// <summary>Parameterized command that upserts one destination row when key columns are available.</summary>
    public string? DestinationMergeCommand { get; set; }
}
