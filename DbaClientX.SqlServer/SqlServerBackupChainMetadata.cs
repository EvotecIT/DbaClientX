namespace DBAClientX;

/// <summary>Native sequence metadata used to qualify a single-fork disk-backup restore chain.</summary>
public sealed class SqlServerBackupChainMetadata
{
    internal SqlServerBackupChainMetadata(decimal? firstLsn, decimal? lastLsn, decimal? checkpointLsn,
        decimal? databaseBackupLsn, decimal? differentialBaseLsn, System.Guid? differentialBaseGuid,
        System.Guid? firstRecoveryForkId, System.Guid? recoveryForkId, decimal? forkPointLsn,
        bool isSnapshot, bool hasIncompleteMetadata)
    {
        FirstLsn = firstLsn; LastLsn = lastLsn; CheckpointLsn = checkpointLsn;
        DatabaseBackupLsn = databaseBackupLsn; DifferentialBaseLsn = differentialBaseLsn;
        DifferentialBaseGuid = differentialBaseGuid; FirstRecoveryForkId = firstRecoveryForkId;
        RecoveryForkId = recoveryForkId; ForkPointLsn = forkPointLsn;
        IsSnapshot = isSnapshot; HasIncompleteMetadata = hasIncompleteMetadata;
    }

    /// <summary>First native log sequence number; decimal preserves SQL Server's numeric(25,0) values.</summary>
    public decimal? FirstLsn { get; }
    /// <summary>Native sequence number immediately after the last log record in this set.</summary>
    public decimal? LastLsn { get; }
    /// <summary>Native checkpoint sequence number.</summary>
    public decimal? CheckpointLsn { get; }
    /// <summary>Most recent conventional full-backup checkpoint; a log need not share the selected full's value.</summary>
    public decimal? DatabaseBackupLsn { get; }
    /// <summary>Native differential-base sequence number; null for non-differentials or multi-based differentials.</summary>
    public decimal? DifferentialBaseLsn { get; }
    /// <summary>Backup-set identity of a single differential base.</summary>
    public System.Guid? DifferentialBaseGuid { get; }
    /// <summary>Starting native recovery fork.</summary>
    public System.Guid? FirstRecoveryForkId { get; }
    /// <summary>Ending native recovery fork.</summary>
    public System.Guid? RecoveryForkId { get; }
    /// <summary>Native fork transition point, when present; cross-fork chains are unsupported.</summary>
    public decimal? ForkPointLsn { get; }
    /// <summary>Whether the backup is a snapshot; snapshot chains are unsupported.</summary>
    public bool IsSnapshot { get; }
    /// <summary>Whether SQL Server reports incomplete backup metadata.</summary>
    public bool HasIncompleteMetadata { get; }
}
