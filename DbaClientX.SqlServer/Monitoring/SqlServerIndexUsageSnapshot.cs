using System;
using System.Collections.Generic;

namespace DBAClientX.SqlServerMonitoring;

/// <summary>A bounded metadata observation for one database; this is evidence for assessment, not an index-removal recommendation.</summary>
public sealed class SqlServerIndexUsageSnapshot
{
    internal SqlServerIndexUsageSnapshot(string database, IReadOnlyList<SqlServerIndexUsage> indexes,
        bool truncated, DateTime? serverStartTime)
    {
        DatabaseName = database;
        Indexes = indexes;
        IsTruncated = truncated;
        ServerStartTime = serverStartTime;
        CollectedUtc = DateTimeOffset.UtcNow;
    }

    /// <summary>Database queried, rather than all databases on the instance.</summary>
    public string DatabaseName { get; }
    /// <summary>Visible rowstore table indexes in deterministic schema/table/index order; excludes indexed views, heaps, columnstore and memory-optimized indexes.</summary>
    public IReadOnlyList<SqlServerIndexUsage> Indexes { get; }
    /// <summary>
    /// Whether more visible indexes exist than the requested row limit. A false value does not establish
    /// database-wide catalog visibility: caller permissions can hide objects even in an empty result.
    /// </summary>
    public bool IsTruncated { get; }
    /// <summary>UTC time when this collection completed.</summary>
    public DateTimeOffset CollectedUtc { get; }
    /// <summary>
    /// Instance start in SQL Server's local time, available when at least one index was returned.
    /// This is an upper bound on counter history; detach/shutdown and index changes can reset counters later.
    /// </summary>
    public DateTime? ServerStartTime { get; }
}
