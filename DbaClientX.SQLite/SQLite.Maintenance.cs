using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Provides SQLite maintenance helpers for checkpointing and graceful shutdown preparation.
/// </summary>
public partial class SQLite
{
    /// <summary>
    /// Copies a SQLite database into a destination database using SQLite's online backup API.
    /// </summary>
    /// <param name="sourceDatabase">Absolute or relative path of the source SQLite database file.</param>
    /// <param name="destinationDatabase">Absolute or relative path of the destination SQLite database file.</param>
    /// <param name="busyTimeoutMs">Optional positive busy timeout in milliseconds applied to both connections.</param>
    /// <remarks>
    /// The source database is opened read-only and the destination is created when it does not exist. This is
    /// intended for backup-first maintenance workflows that need a provider-owned copy operation without exposing
    /// <c>Microsoft.Data.Sqlite</c> objects to consumer projects.
    /// </remarks>
    public virtual void BackupDatabase(
        string sourceDatabase,
        string destinationDatabase,
        int? busyTimeoutMs = null)
        => BackupDatabase(sourceDatabase, destinationDatabase, overwriteDestination: false, busyTimeoutMs);

    /// <summary>
    /// Copies a SQLite database into a destination database using SQLite's online backup API.
    /// </summary>
    /// <param name="sourceDatabase">Absolute or relative path of the source SQLite database file.</param>
    /// <param name="destinationDatabase">Absolute or relative path of the destination SQLite database file.</param>
    /// <param name="overwriteDestination">Whether an existing destination may be atomically replaced.</param>
    /// <param name="busyTimeoutMs">Optional positive busy timeout in milliseconds applied to both connections.</param>
    /// <remarks>
    /// The source database is opened read-only. The destination is created when it does not exist and is replaced
    /// atomically only when <paramref name="overwriteDestination"/> is true.
    /// </remarks>
    public virtual void BackupDatabase(
        string sourceDatabase,
        string destinationDatabase,
        bool overwriteDestination,
        int? busyTimeoutMs = null)
    {
        ValidateDatabasePath(sourceDatabase);
        ValidateDatabasePath(destinationDatabase);
        EnsureNoActiveTransaction();
        if (busyTimeoutMs is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(busyTimeoutMs), "Busy timeout must be positive when specified.");
        }

        string sourcePath = Path.GetFullPath(sourceDatabase);
        string destinationPath = Path.GetFullPath(destinationDatabase);
        if (AreSameBackupPath(sourcePath, destinationPath))
        {
            throw new ArgumentException("Source and destination database paths must be different.", nameof(destinationDatabase));
        }
        if (File.Exists(sourcePath) && !overwriteDestination && File.Exists(destinationPath))
        {
            throw new IOException($"SQLite backup destination already exists: {destinationPath}");
        }

        var options = new SqliteBackupOptions
        {
            OverwriteDestination = overwriteDestination
        };
        if (busyTimeoutMs.HasValue)
        {
            options.BusyRetryTimeout = TimeSpan.FromMilliseconds(busyTimeoutMs.Value);
        }
        try
        {
            BackupDatabaseIncrementalAsync(sourceDatabase, destinationDatabase, options)
                .GetAwaiter()
                .GetResult();
        }
        catch (DbaQueryExecutionException)
        {
            throw;
        }
        catch (Exception exception) when (
            exception is SqliteException or IOException or UnauthorizedAccessException or
            InvalidOperationException or NotSupportedException or TimeoutException)
        {
            throw CreateQueryExecutionException(
                "Failed to back up SQLite database.",
                "SQLite online backup",
                exception);
        }
    }

    /// <summary>
    /// Executes <c>PRAGMA wal_checkpoint(...)</c> using the supplied checkpoint mode.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="mode">Checkpoint mode to apply.</param>
    /// <param name="cancellationToken">Token used to cancel command execution.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout in milliseconds.</param>
    /// <returns>A task that completes when the checkpoint has finished.</returns>
    public virtual Task CheckpointAsync(
        string database,
        SqliteCheckpointMode mode = SqliteCheckpointMode.Passive,
        CancellationToken cancellationToken = default,
        int? busyTimeoutMs = null)
    {
        string checkpoint = mode switch
        {
            SqliteCheckpointMode.Full => "FULL",
            SqliteCheckpointMode.Restart => "RESTART",
            SqliteCheckpointMode.Truncate => "TRUNCATE",
            _ => "PASSIVE"
        };

        return ExecuteMaintenancePragmaAsync(
            database,
            $"PRAGMA wal_checkpoint({checkpoint});",
            cancellationToken,
            busyTimeoutMs);
    }

    /// <summary>
    /// Executes <c>PRAGMA optimize</c> against the supplied SQLite database file.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="cancellationToken">Token used to cancel command execution.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout in milliseconds.</param>
    /// <returns>A task that completes when optimization has finished.</returns>
    public virtual Task OptimizeAsync(
        string database,
        CancellationToken cancellationToken = default,
        int? busyTimeoutMs = null)
    {
        return ExecuteMaintenancePragmaAsync(
            database,
            "PRAGMA optimize;",
            cancellationToken,
            busyTimeoutMs);
    }

    /// <summary>
    /// Performs best-effort SQLite maintenance suitable for a graceful application shutdown.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="options">Optional shutdown maintenance settings.</param>
    /// <param name="cancellationToken">Token used to cancel command execution.</param>
    /// <returns>A task that completes when shutdown maintenance has finished.</returns>
    public virtual async Task PrepareForShutdownAsync(
        string database,
        SqliteShutdownMaintenanceOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        EnsureNoActiveTransaction();
        var effectiveOptions = options ?? new SqliteShutdownMaintenanceOptions();

        await CheckpointAsync(
                database,
                effectiveOptions.CheckpointMode,
                cancellationToken,
                effectiveOptions.BusyTimeoutMs)
            .ConfigureAwait(false);

        if (effectiveOptions.OptimizeAfterCheckpoint)
        {
            await OptimizeAsync(database, cancellationToken, effectiveOptions.BusyTimeoutMs).ConfigureAwait(false);
        }
    }

    private Task ExecuteMaintenancePragmaAsync(
        string database,
        string pragma,
        CancellationToken cancellationToken,
        int? busyTimeoutMs)
    {
        ValidateDatabasePath(database);
        ValidateCommandText(pragma);
        EnsureNoActiveTransaction();
        EnsureMaintenanceDatabaseExists(database);

        return RunDedicatedMaintenanceAsync(
            () =>
            {
                ExecuteMaintenancePragmaCore(database, pragma, busyTimeoutMs, cancellationToken);
                return true;
            },
            cancellationToken);
    }

    private void ExecuteMaintenancePragmaCore(
        string database,
        string pragma,
        int? busyTimeoutMs,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            using var connection = new SqliteConnection(BuildOperationalConnectionString(database));
            connection.Open();
            ApplyBusyTimeout(connection, busyTimeoutMs);
            using CancellationTokenRegistration registration = cancellationToken.Register(
                static state => SQLitePCL.raw.sqlite3_interrupt(((SqliteConnection)state!).Handle),
                connection);

            using var command = connection.CreateCommand();
            command.CommandText = pragma;
            ApplyCommandTimeout(command);

            command.ExecuteNonQuery();
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (SqliteException ex) when (
            cancellationToken.IsCancellationRequested &&
            IsProviderCancellationException(ex))
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw CreateQueryExecutionException("Failed to execute SQLite maintenance command.", pragma, ex);
        }
    }

    private void EnsureNoActiveTransaction()
    {
        lock (_syncRoot)
        {
            if (_transaction != null || _transactionInitializing)
            {
                throw new DbaTransactionException(
                    "SQLite maintenance cannot run while a transaction is active or starting.");
            }
        }
    }

    private static void EnsureMaintenanceDatabaseExists(string database)
    {
        if (string.Equals(database, ":memory:", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var path = database;
        if (Uri.TryCreate(database, UriKind.Absolute, out var uri) && uri.IsFile)
        {
            path = uri.LocalPath;
        }

        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"SQLite database file does not exist: {path}", path);
        }
    }
}
