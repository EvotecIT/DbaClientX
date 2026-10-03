using System;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>
/// Catalog identity, nullable usage counters and statistics for one visible rowstore index.
/// Missing counters are unknown, not evidence that an index is unused. Usage can reset on restart,
/// database detach, shutdown or index changes; SQL Server does not report a complete per-index observation window.
/// </summary>
public sealed class SqlServerIndexUsage
{
    /// <summary>Schema containing the table.</summary>
    public string SchemaName { get; set; } = string.Empty;
    /// <summary>Table containing the index.</summary>
    public string TableName { get; set; } = string.Empty;
    /// <summary>Index name.</summary>
    public string IndexName { get; set; } = string.Empty;
    /// <summary>Database object identifier; useful for matching consecutive observations.</summary>
    public int ObjectId { get; set; }
    /// <summary>Index identifier within the object.</summary>
    public int IndexId { get; set; }
    /// <summary>Whether the index enforces a primary key.</summary>
    public bool IsPrimaryKey { get; set; }
    /// <summary>Whether the index enforces uniqueness.</summary>
    public bool IsUnique { get; set; }
    /// <summary>Whether the index implements a unique constraint.</summary>
    public bool IsUniqueConstraint { get; set; }
    /// <summary>Whether the index is disabled.</summary>
    public bool IsDisabled { get; set; }
    /// <summary>User seeks, or null when SQL Server has no usage row.</summary>
    public long? UserSeeks { get; set; }
    /// <summary>User scans, or null when SQL Server has no usage row.</summary>
    public long? UserScans { get; set; }
    /// <summary>User lookups, or null when SQL Server has no usage row.</summary>
    public long? UserLookups { get; set; }
    /// <summary>User update operations, not changed-row counts; null when SQL Server has no usage row.</summary>
    public long? UserUpdates { get; set; }
    /// <summary>Last user seek in SQL Server's local time, without an inferred UTC offset.</summary>
    public DateTime? LastUserSeek { get; set; }
    /// <summary>Last user scan in SQL Server's local time, without an inferred UTC offset.</summary>
    public DateTime? LastUserScan { get; set; }
    /// <summary>Last user lookup in SQL Server's local time, without an inferred UTC offset.</summary>
    public DateTime? LastUserLookup { get; set; }
    /// <summary>Last user update in SQL Server's local time, without an inferred UTC offset.</summary>
    public DateTime? LastUserUpdate { get; set; }
    /// <summary>Index statistics update time in SQL Server's local time; null if no statistics blob or properties are visible.</summary>
    public DateTime? StatisticsLastUpdated { get; set; }
    /// <summary>Rows represented by index statistics, or null when properties are unavailable.</summary>
    public long? StatisticsRows { get; set; }
    /// <summary>Rows sampled when creating the index statistics, or null when properties are unavailable.</summary>
    public long? StatisticsRowsSampled { get; set; }
    /// <summary>Changes to the leading statistics column since its last update; null when properties are unavailable.</summary>
    public long? StatisticsModificationCounter { get; set; }
}
