using System;

namespace DBAClientX;

/// <summary>Expected source and media identity retained for a pinned SQL Server restore.</summary>
public sealed class SqlServerBackupIdentity
{
    /// <summary>Creates an identity from a previously verified backup header.</summary>
    /// <param name="sourceDatabaseName">Name of the database in the backup set.</param>
    /// <param name="backupSetGuid">Unique backup-set identity.</param>
    /// <param name="mediaName">Named media label checked by SQL Server during restore.</param>
    /// <param name="mediaSetId">Unique media-set identity.</param>
    public SqlServerBackupIdentity(string sourceDatabaseName, Guid backupSetGuid,
        string mediaName, Guid mediaSetId)
    {
        if (string.IsNullOrWhiteSpace(sourceDatabaseName) || sourceDatabaseName.Length > 128)
        {
            throw new ArgumentException("A source database name of at most 128 characters is required.",
                nameof(sourceDatabaseName));
        }

        if (backupSetGuid == Guid.Empty)
        {
            throw new ArgumentException("A backup-set identity is required.", nameof(backupSetGuid));
        }

        if (string.IsNullOrWhiteSpace(mediaName) || mediaName.Length > 128)
        {
            throw new ArgumentException("A named media label of at most 128 characters is required.",
                nameof(mediaName));
        }

        if (mediaSetId == Guid.Empty)
        {
            throw new ArgumentException("A media-set identity is required.", nameof(mediaSetId));
        }

        SourceDatabaseName = sourceDatabaseName;
        BackupSetGuid = backupSetGuid;
        MediaName = mediaName;
        MediaSetId = mediaSetId;
    }

    /// <summary>Name of the backed-up database.</summary>
    public string SourceDatabaseName { get; }

    /// <summary>Unique backup-set identity.</summary>
    public Guid BackupSetGuid { get; }

    /// <summary>Media name supplied to SQL Server's restore command.</summary>
    public string MediaName { get; }

    /// <summary>Unique media-set identity from the media label.</summary>
    public Guid MediaSetId { get; }
}
