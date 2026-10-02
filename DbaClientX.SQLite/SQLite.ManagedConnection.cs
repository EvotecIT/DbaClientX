using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Opens a provider-managed connection for a domain workflow that still needs direct ADO.NET commands.
    /// </summary>
    /// <remarks>
    /// Prefer the command and session APIs when possible. This method keeps provider creation and operational
    /// pragmas inside DBAClientX while allowing consumers to own domain schema and SQL migrations.
    /// </remarks>
    public virtual DbConnection OpenDbConnection(string database, SQLiteConnectionOptions? options = null)
    {
        options = (options ?? _connectionOptions ?? new SQLiteConnectionOptions()).Snapshot();
        var connectionString = BuildOperationalConnectionString(database, options.ReadOnly);
        var connection = CreateConfiguredConnection(connectionString, options);
        try
        {
            connection.Open();
            ApplyManagedConnectionOptions(connection, options);
            ApplyConnectionConfiguration(connection, applyProfile: false);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    /// <summary>Begins a provider-managed transaction on a connection opened by <see cref="OpenDbConnection"/>.</summary>
    /// <param name="connection">The open managed SQLite connection.</param>
    /// <param name="mode">Whether SQLite should acquire write intent immediately or defer until commands require it.</param>
    /// <returns>The provider-neutral transaction owned by the caller.</returns>
    /// <remarks>
    /// Use <see cref="SQLiteTransactionMode.Deferred"/> for consistent read snapshots that must not reserve
    /// the writer slot before their first command. Commands must set their <see cref="DbCommand.Transaction"/>
    /// property to the returned transaction.
    /// </remarks>
    public virtual DbTransaction BeginDbTransaction(
        DbConnection connection,
        SQLiteTransactionMode mode = SQLiteTransactionMode.Immediate)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }
        if (connection is not SqliteConnection sqliteConnection)
        {
            throw new ArgumentException(
                "The connection must be a managed Microsoft.Data.Sqlite connection.",
                nameof(connection));
        }
        if (sqliteConnection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("The SQLite connection must be open before beginning a transaction.");
        }

        return mode switch
        {
            SQLiteTransactionMode.Immediate => sqliteConnection.BeginTransaction(),
            SQLiteTransactionMode.Deferred => sqliteConnection.BeginTransaction(deferred: true),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unsupported SQLite transaction mode.")
        };
    }

    private void ApplyManagedConnectionOptions(SqliteConnection connection, SQLiteConnectionOptions options, bool applyBusyTimeout = true)
    {
        int busyTimeout = options.BusyTimeoutMs ?? BusyTimeoutMs;
        using var command = connection.CreateCommand();
        ApplyCommandTimeout(command);
        var sql = new System.Text.StringBuilder();
        bool readOnly = new SqliteConnectionStringBuilder(connection.ConnectionString).Mode == SqliteOpenMode.ReadOnly;
        if (applyBusyTimeout && busyTimeout > 0)
        {
            sql.Append("PRAGMA busy_timeout = ").Append(busyTimeout).AppendLine(";");
        }
        if (!readOnly && options.EnableWriteAheadLogging)
        {
            sql.AppendLine("PRAGMA journal_mode = WAL;");
        }
        if (!readOnly && options.UseNormalSynchronousMode)
        {
            sql.AppendLine("PRAGMA synchronous = NORMAL;");
        }
        if (!readOnly && options.WalAutoCheckpointPages > 0)
        {
            sql.Append("PRAGMA wal_autocheckpoint = ").Append(options.WalAutoCheckpointPages).AppendLine(";");
        }
        if (options.UseMemoryTempStore)
        {
            sql.AppendLine("PRAGMA temp_store = MEMORY;");
        }
        if (options.CacheSize != 0)
        {
            sql.Append("PRAGMA cache_size = ").Append(options.CacheSize).AppendLine(";");
        }
        sql.Append("PRAGMA foreign_keys = ").Append(options.EnableForeignKeys ? "ON" : "OFF").AppendLine(";");
        command.CommandText = sql.ToString();
        command.ExecuteNonQuery();
    }
}
