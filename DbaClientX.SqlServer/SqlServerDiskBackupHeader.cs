using System;

namespace DBAClientX;

/// <summary>Identity and safety metadata from one SQL Server disk backup set.</summary>
public sealed class SqlServerDiskBackupHeader
{
    /// <summary>Creates a header from SQL Server RESTORE HEADERONLY metadata.</summary>
    internal SqlServerDiskBackupHeader(string databaseName, Guid backupSetGuid, Guid? familyGuid,
        string mediaName, Guid mediaSetId, int backupType, bool isCopyOnly,
        bool hasBackupChecksums, bool isDamaged)
    {
        DatabaseName = databaseName;
        BackupSetGuid = backupSetGuid;
        FamilyGuid = familyGuid;
        MediaName = mediaName;
        MediaSetId = mediaSetId;
        Identity = new SqlServerBackupIdentity(databaseName, backupSetGuid, mediaName, mediaSetId);
        BackupType = backupType;
        IsCopyOnly = isCopyOnly;
        HasBackupChecksums = hasBackupChecksums;
        IsDamaged = isDamaged;
    }

    /// <summary>Name of the backed-up database.</summary>
    public string DatabaseName { get; }

    /// <summary>Unique identity of the backup set on its media.</summary>
    public Guid BackupSetGuid { get; }

    /// <summary>Original database family identity, when present in the header.</summary>
    public Guid? FamilyGuid { get; }

    /// <summary>Media name checked by SQL Server during a pinned restore.</summary>
    public string MediaName { get; }

    /// <summary>Identity of the media set containing this backup set.</summary>
    public Guid MediaSetId { get; }

    /// <summary>Source and media identity to retain with a recovery record.</summary>
    public SqlServerBackupIdentity Identity { get; }

    /// <summary>SQL Server backup type; 1 is a full database backup.</summary>
    public int BackupType { get; }

    /// <summary>Whether the backup was created with COPY_ONLY.</summary>
    public bool IsCopyOnly { get; }

    /// <summary>Whether the backup set contains backup checksums.</summary>
    public bool HasBackupChecksums { get; }

    /// <summary>Whether SQL Server marked the backup set as damaged.</summary>
    public bool IsDamaged { get; }
}

/// <summary>A newly created SQL Server backup and its verified backup-set identity.</summary>
public sealed class SqlServerDiskBackupResult
{
    /// <summary>Creates a result for a SQL Server-visible backup file.</summary>
    internal SqlServerDiskBackupResult(string serverBackupPath, SqlServerDiskBackupHeader header)
    {
        ServerBackupPath = serverBackupPath;
        Header = header;
    }

    /// <summary>Path as seen by the SQL Server service account.</summary>
    public string ServerBackupPath { get; }

    /// <summary>Verified identity and safety metadata of the backup set.</summary>
    public SqlServerDiskBackupHeader Header { get; }
}
