using System.Collections.ObjectModel;

namespace DBAClientX;

/// <summary>One dedicated disk backup and its previously retained expected identity.</summary>
public sealed class SqlServerRestoreChainSource
{
    /// <summary>Pins one backup path to a verified source, backup-set and media identity.</summary>
    public SqlServerRestoreChainSource(string serverBackupPath, SqlServerBackupIdentity expectedBackup)
    {
        if (string.IsNullOrWhiteSpace(serverBackupPath) || serverBackupPath.Length > 4000 || serverBackupPath.IndexOf('\0') >= 0)
            throw new ArgumentException("A SQL Server-visible dedicated backup path is required.", nameof(serverBackupPath));
        ServerBackupPath = serverBackupPath;
        ExpectedBackup = expectedBackup ?? throw new ArgumentNullException(nameof(expectedBackup));
    }
    /// <summary>Path as seen by the SQL Server service account.</summary>
    public string ServerBackupPath { get; }
    /// <summary>Expected identity, retained when the backup was verified.</summary>
    public SqlServerBackupIdentity ExpectedBackup { get; }
}

/// <summary>A verified step in an explicitly ordered full, optional differential, and log chain.</summary>
public sealed class SqlServerRestoreChainStep
{
    internal SqlServerRestoreChainStep(SqlServerRestoreChainSource source, SqlServerDiskBackupHeader header)
    { Source = source; Header = header; }
    /// <summary>Pinned backup path and expected identity.</summary>
    public SqlServerRestoreChainSource Source { get; }
    /// <summary>Native header qualified during preparation.</summary>
    public SqlServerDiskBackupHeader Header { get; }
}

/// <summary>An immutable, single-fork restore chain targeting a new database and explicit file locations.</summary>
public sealed class SqlServerRestoreChainPlan
{
    internal SqlServerRestoreChainPlan(SqlServerRestorePlan basePlan, IList<SqlServerRestoreChainStep> steps,
        IReadOnlyList<SqlServerBackupFileInfo> requiredFiles)
    {
        BasePlan = basePlan;
        Steps = new ReadOnlyCollection<SqlServerRestoreChainStep>(steps.ToArray());
        RequiredFiles = Array.AsReadOnly(requiredFiles.ToArray());
        RequiredFileBytes = requiredFiles.Aggregate(0L, (sum, file) => checked(sum + file.SizeBytes));
        PreparedAtUtc = DateTimeOffset.UtcNow;
    }
    internal SqlServerRestorePlan BasePlan { get; }
    /// <summary>New target database; existing databases are refused.</summary>
    public string TargetDatabaseName => BasePlan.TargetDatabaseName;
    /// <summary>Ordered, pinned backup sets. The plan never chooses or sorts a different chain.</summary>
    public IReadOnlyList<SqlServerRestoreChainStep> Steps { get; }
    /// <summary>Explicit relocation inherited from the base full backup.</summary>
    public IReadOnlyDictionary<string, string> FileDestinations => BasePlan.FileDestinations;
    /// <summary>Maximum recorded allocation per logical file across the chain.</summary>
    public IReadOnlyList<SqlServerBackupFileInfo> RequiredFiles { get; }
    /// <summary>Sum of recorded maximum allocations; excludes growth, CHECKDB and temporary space. Capacity is not reserved.</summary>
    public long RequiredFileBytes { get; }
    /// <summary>Preparation completion time; backup media and target state are revalidated before execution.</summary>
    public DateTimeOffset PreparedAtUtc { get; }
}
