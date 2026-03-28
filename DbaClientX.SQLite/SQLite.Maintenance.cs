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

    private async Task ExecuteMaintenancePragmaAsync(
        string database,
        string pragma,
        CancellationToken cancellationToken,
        int? busyTimeoutMs)
    {
        ValidateDatabasePath(database);
        ValidateCommandText(pragma);
        EnsureNoActiveTransaction();

        SqliteConnection? connection = null;
        try
        {
            connection = new SqliteConnection(BuildOperationalConnectionString(database));
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ApplyBusyTimeoutAsync(connection, busyTimeoutMs, cancellationToken).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = pragma;
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute SQLite maintenance command.", pragma, ex);
        }
        finally
        {
            if (connection != null)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }

    private void EnsureNoActiveTransaction()
    {
        if (IsInTransaction)
        {
            throw new DbaTransactionException("SQLite maintenance cannot run while a transaction is active.");
        }
    }
}
