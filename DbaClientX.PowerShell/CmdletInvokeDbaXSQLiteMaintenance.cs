namespace DBAClientX.PowerShell;

/// <summary>Runs SQLite maintenance operations through the DbaClientX SQLite provider.</summary>
/// <example>
/// <summary>Prepare a SQLite database for shutdown.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXSQLiteMaintenance -Database .\app.db -Action PrepareForShutdown</code>
/// <para>Runs the provider shutdown maintenance sequence.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXSQLiteMaintenance", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXSQLiteMaintenance : AsyncPSCmdlet
{
    /// <summary>SQLite database path or SQLite connection string.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    [Alias("Path", "ConnectionString")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>Maintenance operation to execute.</summary>
    [Parameter(Mandatory = true, Position = 1)]
    public DbaXSQLiteMaintenanceAction Action { get; set; }

    /// <summary>Destination database path for the Backup action.</summary>
    [Parameter(Mandatory = false)]
    public string? Destination { get; set; }

    /// <summary>Checkpoint mode used by Checkpoint and PrepareForShutdown.</summary>
    [Parameter(Mandatory = false)]
    public SqliteCheckpointMode CheckpointMode { get; set; } = SqliteCheckpointMode.Truncate;

    /// <summary>Optional busy timeout in milliseconds.</summary>
    [Parameter(Mandatory = false)]
    public int? BusyTimeoutMs { get; set; }

    /// <summary>Skips PRAGMA optimize after checkpointing during PrepareForShutdown.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter SkipOptimize { get; set; }

    /// <summary>Returns a small completion object.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter PassThru { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        if (BusyTimeoutMs.HasValue && BusyTimeoutMs.Value < 0)
        {
            throw new PSArgumentException("BusyTimeoutMs cannot be negative.", nameof(BusyTimeoutMs));
        }

        var database = DbaXProviderHelpers.GetSQLiteDatabasePath(Database, "SQLite maintenance");
        string? destination = null;
        if (Action == DbaXSQLiteMaintenanceAction.Backup && string.IsNullOrWhiteSpace(Destination))
        {
            throw new PSArgumentException("Destination is required for SQLite backup maintenance.", nameof(Destination));
        }

        if (Action == DbaXSQLiteMaintenanceAction.Backup)
        {
            destination = DbaXProviderHelpers.GetSQLiteDatabasePath(Destination!, "SQLite backup destination");
        }

        if (!ShouldProcess(database, $"Run SQLite {Action} maintenance"))
        {
            return;
        }

        using var client = new DBAClientX.SQLite();
        switch (Action)
        {
            case DbaXSQLiteMaintenanceAction.Backup:
                client.BackupDatabase(database, destination!, BusyTimeoutMs);
                break;
            case DbaXSQLiteMaintenanceAction.Checkpoint:
                await client.CheckpointAsync(database, CheckpointMode, CancelToken, BusyTimeoutMs).ConfigureAwait(false);
                break;
            case DbaXSQLiteMaintenanceAction.Optimize:
                await client.OptimizeAsync(database, CancelToken, BusyTimeoutMs).ConfigureAwait(false);
                break;
            case DbaXSQLiteMaintenanceAction.PrepareForShutdown:
                await client.PrepareForShutdownAsync(database, new SqliteShutdownMaintenanceOptions
                {
                    BusyTimeoutMs = BusyTimeoutMs,
                    CheckpointMode = CheckpointMode,
                    OptimizeAfterCheckpoint = !SkipOptimize.IsPresent
                }, CancelToken).ConfigureAwait(false);
                break;
            default:
                throw new PSArgumentException($"SQLite maintenance action '{Action}' is not supported.", nameof(Action));
        }

        if (PassThru.IsPresent)
        {
            WriteObject(new PSObject(new
            {
                Database = database,
                Action,
                Completed = true,
                CompletedAt = DateTimeOffset.UtcNow
            }));
        }
    }
}
