namespace DBAClientX;

/// <summary>A database file recorded in a SQL Server disk backup set.</summary>
public sealed class SqlServerBackupFileInfo
{
    /// <summary>Creates a backup-file description from SQL Server metadata.</summary>
    public SqlServerBackupFileInfo(string logicalName, string originalPhysicalName, string fileType)
        : this(logicalName, originalPhysicalName, fileType, 0)
    {
    }

    /// <summary>Creates a backup-file description including the allocated size in bytes.</summary>
    public SqlServerBackupFileInfo(string logicalName, string originalPhysicalName, string fileType, long sizeBytes)
    {
        if (sizeBytes < 0) throw new System.ArgumentOutOfRangeException(nameof(sizeBytes));
        LogicalName = logicalName;
        OriginalPhysicalName = originalPhysicalName;
        FileType = fileType;
        SizeBytes = sizeBytes;
    }

    /// <summary>Logical file name used by RESTORE MOVE.</summary>
    public string LogicalName { get; }

    /// <summary>Physical path recorded when the backup was created.</summary>
    public string OriginalPhysicalName { get; }

    /// <summary>SQL Server file type, such as D for data or L for log.</summary>
    public string FileType { get; }

    /// <summary>Allocated size from FILELISTONLY, or zero when not supplied to the constructor.</summary>
    public long SizeBytes { get; }
}
