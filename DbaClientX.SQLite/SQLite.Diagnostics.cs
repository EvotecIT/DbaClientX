using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Collects read-only SQLite health and file-state diagnostics for a database file.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <param name="cancellationToken">Token used to cancel command execution.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout in milliseconds.</param>
    /// <returns>A typed diagnostics snapshot.</returns>
    public virtual async Task<SqliteDatabaseDiagnostics> CollectDiagnosticsAsync(
        string database,
        CancellationToken cancellationToken = default,
        int? busyTimeoutMs = null)
    {
        ValidateDatabasePath(database);
        string fullPath = Path.GetFullPath(database);
        var diagnostics = new SqliteDatabaseDiagnostics
        {
            Database = database,
            FullPath = fullPath
        };

        ApplyFileDiagnostics(diagnostics, fullPath);
        if (!diagnostics.Exists)
        {
            diagnostics.ErrorMessage = "SQLite database file does not exist.";
            return diagnostics;
        }

        try
        {
            using var connection = new SqliteConnection(BuildConnectionString(fullPath, readOnly: true, busyTimeoutMs: busyTimeoutMs));
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ApplyBusyTimeoutAsync(connection, busyTimeoutMs, cancellationToken).ConfigureAwait(false);

            diagnostics.CanConnect = true;
            diagnostics.SQLiteVersion = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "SELECT sqlite_version();", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.IntegrityCheck = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "PRAGMA integrity_check;", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.QuickCheck = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "PRAGMA quick_check;", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.JournalMode = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "PRAGMA journal_mode;", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.SynchronousMode = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "PRAGMA synchronous;", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.LockingMode = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "PRAGMA locking_mode;", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.AutoVacuumMode = Convert.ToString(
                await ExecuteDiagnosticScalarAsync(connection, "PRAGMA auto_vacuum;", cancellationToken).ConfigureAwait(false),
                CultureInfo.InvariantCulture);
            diagnostics.PageCount = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA page_count;", cancellationToken).ConfigureAwait(false));
            diagnostics.PageSizeBytes = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA page_size;", cancellationToken).ConfigureAwait(false));
            diagnostics.FreelistCount = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA freelist_count;", cancellationToken).ConfigureAwait(false));
            diagnostics.SchemaVersion = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA schema_version;", cancellationToken).ConfigureAwait(false));
            diagnostics.UserVersion = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA user_version;", cancellationToken).ConfigureAwait(false));
            diagnostics.ApplicationId = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA application_id;", cancellationToken).ConfigureAwait(false));
            diagnostics.WalAutoCheckpointPages = ConvertToNullableInt64(await ExecuteDiagnosticScalarAsync(connection, "PRAGMA wal_autocheckpoint;", cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (SqliteException ex)
        {
            diagnostics.ErrorMessage = ex.Message;
        }
        catch (IOException ex)
        {
            diagnostics.ErrorMessage = ex.Message;
        }
        catch (UnauthorizedAccessException ex)
        {
            diagnostics.ErrorMessage = ex.Message;
        }

        return diagnostics;
    }

    private static void ApplyFileDiagnostics(SqliteDatabaseDiagnostics diagnostics, string fullPath)
    {
        var databaseFile = new FileInfo(fullPath);
        diagnostics.Exists = databaseFile.Exists;
        if (databaseFile.Exists)
        {
            diagnostics.DatabaseFileSizeBytes = databaseFile.Length;
            diagnostics.LastWriteTimeUtc = new DateTimeOffset(databaseFile.LastWriteTimeUtc, TimeSpan.Zero);
        }

        diagnostics.WalFileSizeBytes = GetFileSize(fullPath + "-wal");
        diagnostics.SharedMemoryFileSizeBytes = GetFileSize(fullPath + "-shm");
    }

    private static long GetFileSize(string path)
    {
        var file = new FileInfo(path);
        return file.Exists ? file.Length : 0L;
    }

    private async Task<object?> ExecuteDiagnosticScalarAsync(
        SqliteConnection connection,
        string commandText,
        CancellationToken cancellationToken)
    {
        ValidateCommandText(commandText);
        return await ExecuteWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = commandText;
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }

    private static long? ConvertToNullableInt64(object? value)
    {
        if (value is null || value is DBNull)
        {
            return null;
        }

        return Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }
}
