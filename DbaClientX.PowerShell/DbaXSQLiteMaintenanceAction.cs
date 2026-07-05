namespace DBAClientX.PowerShell;

/// <summary>
/// Selects the SQLite maintenance operation executed by Invoke-DbaXSQLiteMaintenance.
/// </summary>
public enum DbaXSQLiteMaintenanceAction
{
    /// <summary>Copy the source database to a destination database using SQLite online backup.</summary>
    Backup,

    /// <summary>Run a WAL checkpoint using the selected checkpoint mode.</summary>
    Checkpoint,

    /// <summary>Run PRAGMA optimize.</summary>
    Optimize,

    /// <summary>Run shutdown-oriented checkpoint and optional optimize operations.</summary>
    PrepareForShutdown
}
