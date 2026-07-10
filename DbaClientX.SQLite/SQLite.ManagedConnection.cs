using System;
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
        options ??= new SQLiteConnectionOptions();
        var connectionString = BuildOperationalConnectionString(database, options.ReadOnly);
        var connection = new SqliteConnection(connectionString);
        try
        {
            connection.Open();
            ApplyManagedConnectionOptions(connection, options);
            return connection;
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private void ApplyManagedConnectionOptions(SqliteConnection connection, SQLiteConnectionOptions options)
    {
        int busyTimeout = ResolveBusyTimeoutMs(options.BusyTimeoutMs);
        using var command = connection.CreateCommand();
        var sql = new System.Text.StringBuilder();
        if (busyTimeout > 0)
        {
            sql.Append("PRAGMA busy_timeout = ").Append(busyTimeout).AppendLine(";");
        }
        if (!options.ReadOnly && options.EnableWriteAheadLogging)
        {
            sql.AppendLine("PRAGMA journal_mode = WAL;");
        }
        if (!options.ReadOnly && options.UseNormalSynchronousMode)
        {
            sql.AppendLine("PRAGMA synchronous = NORMAL;");
        }
        if (!options.ReadOnly && options.WalAutoCheckpointPages > 0)
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
