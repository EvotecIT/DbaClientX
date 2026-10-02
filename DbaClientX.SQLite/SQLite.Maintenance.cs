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
    /// <c>Microsoft.Data.Sqlite</c> objects to consumer projects. A WAL database is copied as a held snapshot and any
    /// other database step-wise (<see cref="SqliteBackupMethod.Auto"/>).
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
    /// atomically only when <paramref name="overwriteDestination"/> is true. A WAL database is copied as a held snapshot,
    /// which completes while other connections write, and any other database step-wise
    /// (<see cref="SqliteBackupMethod.Auto"/>, see <see cref="BackupDatabaseAsync"/>).
    /// </remarks>
    public virtual void BackupDatabase(
        string sourceDatabase,
        string destinationDatabase,
        bool overwriteDestination,
        int? busyTimeoutMs = null)
        => BackupDatabase(sourceDatabase, destinationDatabase, overwriteDestination, SqliteBackupMethod.Auto, busyTimeoutMs);

    /// <summary>
    /// Copies a SQLite database into a destination database using SQLite's online backup API and the selected method.
    /// </summary>
    /// <param name="sourceDatabase">Absolute or relative path of the source SQLite database file.</param>
    /// <param name="destinationDatabase">Absolute or relative path of the destination SQLite database file.</param>
    /// <param name="overwriteDestination">Whether an existing destination may be atomically replaced.</param>
    /// <param name="method">How the source is read; <see cref="SqliteBackupMethod.Incremental"/> keeps the step-wise copy
    /// for a rollback-journal database whose writers must not wait for the whole copy.</param>
    /// <param name="busyTimeoutMs">Optional positive busy timeout in milliseconds applied to both connections.</param>
    /// <remarks>
    /// Blocks the calling thread until the copy ends; use <see cref="BackupDatabaseAsync"/> for progress and
    /// cancellation. Errors are reported as <see cref="BackupDatabaseAsync"/> reports them.
    /// </remarks>
    public virtual void BackupDatabase(
        string sourceDatabase,
        string destinationDatabase,
        bool overwriteDestination,
        SqliteBackupMethod method,
        int? busyTimeoutMs = null)
    {
        if (busyTimeoutMs is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(busyTimeoutMs), "Busy timeout must be positive when specified.");
        }

        var options = new SqliteBackupOptions
        {
            OverwriteDestination = overwriteDestination
        };
        if (busyTimeoutMs.HasValue)
        {
            options.BusyRetryTimeout = TimeSpan.FromMilliseconds(busyTimeoutMs.Value);
        }

        BackupDatabaseAsync(sourceDatabase, destinationDatabase, method, options)
            .GetAwaiter()
            .GetResult();
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
            using var connection = CreateConfiguredConnection(BuildOperationalConnectionString(database));
            connection.Open();
            ApplyBusyTimeout(connection, busyTimeoutMs);
            ApplyConnectionConfiguration(connection);
            using CancellationTokenRegistration registration = RegisterStatementInterrupt(connection, cancellationToken);

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
