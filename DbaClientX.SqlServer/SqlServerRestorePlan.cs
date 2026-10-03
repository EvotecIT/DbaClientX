using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace DBAClientX;

/// <summary>A pinned, immutable description of a preflighted restore to a new database.</summary>
public sealed class SqlServerRestorePlan
{
    internal SqlServerRestorePlan(string backupPath, string databaseName, SqlServerDiskBackupHeader header,
        IReadOnlyList<SqlServerBackupFileInfo> files, IReadOnlyDictionary<string, string> destinations)
    {
        ServerBackupPath = backupPath;
        TargetDatabaseName = databaseName;
        Header = header;
        Files = Array.AsReadOnly(files.ToArray());
        FileDestinations = new ReadOnlyDictionary<string, string>(
            destinations.ToDictionary(item => item.Key, item => item.Value, StringComparer.OrdinalIgnoreCase));
        RequiredFileBytes = files.Aggregate(0L, (total, file) => checked(total + file.SizeBytes));
        PreparedAtUtc = DateTimeOffset.UtcNow;
    }

    /// <summary>Backup path as seen by the SQL Server service account.</summary>
    public string ServerBackupPath { get; }
    /// <summary>New destination database name.</summary>
    public string TargetDatabaseName { get; }
    /// <summary>Pinned source, backup-set and media metadata.</summary>
    public SqlServerDiskBackupHeader Header { get; }
    /// <summary>Files and their uncompressed allocated sizes recorded in the backup.</summary>
    public IReadOnlyList<SqlServerBackupFileInfo> Files { get; }
    /// <summary>One immutable destination path per logical file.</summary>
    public IReadOnlyDictionary<string, string> FileDestinations { get; }
    /// <summary>Sum of allocated file sizes; excludes CHECKDB, growth and other temporary space.</summary>
    public long RequiredFileBytes { get; }
    /// <summary>Time at which preparation completed; preflight is not a reservation of names, media or disk space.</summary>
    public DateTimeOffset PreparedAtUtc { get; }
}
