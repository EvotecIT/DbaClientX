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

    /// <summary>
    /// How the Backup action reads the source. Auto (the default) copies a WAL database as a held snapshot, which
    /// completes while other connections write, and any other database step-wise. Snapshot always holds one read
    /// transaction across the copy (in rollback-journal mode writers wait until it ends). Incremental releases the
    /// source between steps, so writers never wait for more than one step, but every write restarts the copy.
    /// </summary>
    [Parameter(Mandatory = false)]
    public SqliteBackupMethod BackupMethod { get; set; } = SqliteBackupMethod.Auto;

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
            if (BusyTimeoutMs == 0)
            {
                throw new PSArgumentException(
                    "BusyTimeoutMs must be positive for SQLite backup maintenance; omit it to use the default busy deadline.",
                    nameof(BusyTimeoutMs));
            }

            destination = DbaXProviderHelpers.GetSQLiteDatabasePath(Destination!, "SQLite backup destination");
        }

        if (!ShouldProcess(database, $"Run SQLite {Action} maintenance"))
        {
            return;
        }

        using var client = new DBAClientX.SQLite();
        SqliteBackupResult? backup = null;
        switch (Action)
        {
            case DbaXSQLiteMaintenanceAction.Backup:
                backup = await BackupAsync(client, database, destination!).ConfigureAwait(false);
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
            var completion = new PSObject(new
            {
                Database = database,
                Action,
                Completed = true,
                CompletedAt = DateTimeOffset.UtcNow
            });
            if (backup != null)
            {
                completion.Properties.Add(new PSNoteProperty("Destination", backup.DestinationDatabase));
                completion.Properties.Add(new PSNoteProperty("BackupMethod", backup.Method));
                completion.Properties.Add(new PSNoteProperty("CopiedPages", backup.CopiedPages));
                completion.Properties.Add(new PSNoteProperty("DestinationLengthBytes", backup.DestinationLengthBytes));
                completion.Properties.Add(new PSNoteProperty("Elapsed", backup.Elapsed));
            }

            WriteObject(completion);
        }
    }

    private async Task<SqliteBackupResult> BackupAsync(DBAClientX.SQLite client, string database, string destination)
    {
        var options = new SqliteBackupOptions();
        if (BusyTimeoutMs.HasValue)
        {
            options.BusyRetryTimeout = TimeSpan.FromMilliseconds(BusyTimeoutMs.Value);
        }

        var progress = new BackupProgressWriter(this, database);
        SqliteBackupResult result = await client.BackupDatabaseAsync(
                database,
                destination,
                BackupMethod,
                options,
                progress,
                CancelToken)
            .ConfigureAwait(false);
        progress.Complete();
        return result;
    }

    /// <summary>
    /// Writes backup progress as it is reported from the backup thread: when the whole percent changes, at most every
    /// 200 ms (and always for 100%), so neither a long copy nor a step-wise copy that keeps restarting floods the host
    /// (Windows PowerShell 5.1 renders every progress record).
    /// </summary>
    private sealed class BackupProgressWriter : IProgress<SqliteBackupProgress>
    {
        private const int ActivityId = 4;
        private static readonly TimeSpan MinimumInterval = TimeSpan.FromMilliseconds(200);
        private readonly CmdletInvokeDbaXSQLiteMaintenance _cmdlet;
        private readonly string _activity;
        private readonly System.Diagnostics.Stopwatch _sinceLastRecord = new();
        private int _lastPercent = -1;

        internal BackupProgressWriter(CmdletInvokeDbaXSQLiteMaintenance cmdlet, string database)
        {
            _cmdlet = cmdlet;
            _activity = $"Backing up {Path.GetFileName(database)}";
        }

        public void Report(SqliteBackupProgress value)
        {
            int percent = Math.Min(100, Math.Max(0, (int)value.PercentComplete));
            if (percent == _lastPercent ||
                (percent < 100 && _sinceLastRecord.IsRunning && _sinceLastRecord.Elapsed < MinimumInterval))
            {
                return;
            }

            _lastPercent = percent;
            _sinceLastRecord.Restart();
            _cmdlet.WriteProgress(new ProgressRecord(
                ActivityId,
                _activity,
                $"{value.CopiedPages} of {value.TotalPages} page(s) copied")
            {
                PercentComplete = percent
            });
        }

        internal void Complete()
            => _cmdlet.WriteProgress(new ProgressRecord(ActivityId, _activity, "Complete")
            {
                RecordType = ProgressRecordType.Completed
            });
    }
}
