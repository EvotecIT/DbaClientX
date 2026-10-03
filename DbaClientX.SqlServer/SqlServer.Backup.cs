using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Creates and verifies a copy-only, checksummed full database backup in a SQL Server-visible directory.
    /// </summary>
    /// <param name="connectionString">Connection to the SQL Server instance; the operation uses its master database.</param>
    /// <param name="databaseName">Database to back up.</param>
    /// <param name="serverBackupDirectory">Existing directory as seen by the SQL Server service account, not necessarily by this process.</param>
    /// <param name="commandTimeoutSeconds">Timeout for each backup and verification command.</param>
    /// <param name="cancellationToken">Cancellation requested by the caller.</param>
    /// <returns>The SQL Server-visible path and verified identity of the new backup set.</returns>
    /// <remarks>
    /// A random filename avoids modifying a caller-supplied backup file. SQL Server writes the file on its own host;
    /// callers must arrange access to it separately. A failed or cancelled backup can leave an incomplete file at
    /// the generated path, which an operator should remove only after confirming no recovery process needs it.
    /// Verification checks backup readability and checksums, but it is not a test restore or database integrity check.
    /// </remarks>
    public virtual Task<SqlServerDiskBackupResult> BackupDatabaseCopyOnlyToDiskAsync(
        string connectionString,
        string databaseName,
        string serverBackupDirectory,
        int commandTimeoutSeconds = 3600,
        CancellationToken cancellationToken = default)
        => BackupToDedicatedDiskAsync(connectionString, databaseName, serverBackupDirectory,
            SqlServerDiskBackupKind.Full, copyOnly: true, commandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// Checks that the first backup set on a SQL Server-visible disk file is readable and its checksums are valid.
    /// </summary>
    /// <param name="connectionString">Connection to the SQL Server instance that can read the file.</param>
    /// <param name="serverBackupPath">Backup path as seen by the SQL Server service account.</param>
    /// <param name="commandTimeoutSeconds">Verification command timeout.</param>
    /// <param name="cancellationToken">Cancellation requested by the caller.</param>
    /// <remarks>This does not restore the database or establish the integrity of application-owned files.</remarks>
    public virtual Task VerifyDiskBackupAsync(
        string connectionString,
        string serverBackupPath,
        int commandTimeoutSeconds = 3600,
        CancellationToken cancellationToken = default)
    {
        ValidateDiskBackupPath(connectionString, serverBackupPath, commandTimeoutSeconds);
        return ExecuteBackupCommandAsync(
            connectionString,
            "RESTORE VERIFYONLY FROM DISK = @backupPath WITH FILE = 1, CHECKSUM, STOP_ON_ERROR",
            null,
            serverBackupPath,
            null,
            commandTimeoutSeconds,
            cancellationToken);
    }

    private async Task ExecuteBackupCommandAsync(
        string connectionString,
        string commandText,
        string? databaseName,
        string backupPath,
        string? mediaName,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand(commandText, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add("@backupPath", SqlDbType.NVarChar, 4000).Value = backupPath;
        if (databaseName is not null)
        {
            command.Parameters.Add("@databaseName", SqlDbType.NVarChar, 128).Value = databaseName;
        }

        if (mediaName is not null)
        {
            command.Parameters.Add("@mediaName", SqlDbType.NVarChar, 128).Value = mediaName;
        }

        await ExecuteRecoveryNonQueryAsync(command, cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateBackupArguments(
        string connectionString,
        string databaseName,
        string serverBackupDirectory,
        int commandTimeoutSeconds)
    {
        ValidateConnectionString(connectionString);
        if (string.IsNullOrWhiteSpace(databaseName) || databaseName.Length > 128 || databaseName.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("A database name of at most 128 characters is required.", nameof(databaseName));
        }

        if (string.IsNullOrWhiteSpace(serverBackupDirectory) || serverBackupDirectory.IndexOf('\0') >= 0
            || serverBackupDirectory.Length > 3900)
        {
            throw new ArgumentException("A SQL Server-visible backup directory is required.", nameof(serverBackupDirectory));
        }

        ValidateBackupTimeout(commandTimeoutSeconds);
    }

    private static void ValidateBackupTimeout(int commandTimeoutSeconds)
    {
        if (commandTimeoutSeconds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(commandTimeoutSeconds));
        }
    }
}
