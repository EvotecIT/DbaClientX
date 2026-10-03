using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX;

/// <summary>The result of a completed CHECKDB operation; it does not establish application readiness.</summary>
public sealed class SqlServerIntegrityCheckResult
{
    internal SqlServerIntegrityCheckResult(string databaseName, bool physicalOnly, TimeSpan duration,
        long issueCount, IReadOnlyList<SqlServerIntegrityIssue> issues)
    {
        DatabaseName = databaseName;
        PhysicalOnly = physicalOnly;
        Duration = duration;
        IssueCount = issueCount;
        Issues = Array.AsReadOnly(issues.ToArray());
    }

    /// <summary>The checked database.</summary>
    public string DatabaseName { get; }

    /// <summary>Whether only physical checks ran.</summary>
    public bool PhysicalOnly { get; }

    /// <summary>Elapsed CHECKDB execution and diagnostic consumption time.</summary>
    public TimeSpan Duration { get; }

    /// <summary>Total diagnostic records emitted by CHECKDB, including records beyond the storage limit.</summary>
    public long IssueCount { get; }

    /// <summary>Bounded immutable diagnostic records.</summary>
    public IReadOnlyList<SqlServerIntegrityIssue> Issues { get; }

    /// <summary>Whether some diagnostic records were counted without being retained.</summary>
    public bool IssuesTruncated => IssueCount > Issues.Count;

    /// <summary>Whether CHECKDB completed without reporting integrity issues.</summary>
    public bool Succeeded => IssueCount == 0;
}

/// <summary>A CHECKDB diagnostic record with optional message text.</summary>
public sealed class SqlServerIntegrityIssue
{
    internal SqlServerIntegrityIssue(int errorNumber, int severity, int state, string? message)
    {
        ErrorNumber = errorNumber;
        Severity = severity;
        State = state;
        Message = message;
    }

    /// <summary>SQL Server's diagnostic number.</summary>
    public int ErrorNumber { get; }

    /// <summary>SQL Server's diagnostic severity.</summary>
    public int Severity { get; }

    /// <summary>SQL Server's diagnostic state.</summary>
    public int State { get; }

    /// <summary>Diagnostic text only when explicitly requested; null by default.</summary>
    public string? Message { get; }
}
