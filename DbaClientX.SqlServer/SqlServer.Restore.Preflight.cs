using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Preflights a pinned full backup and explicit relocation without restoring or creating a database.</summary>
    /// <param name="connectionString">Connection to the destination instance.</param>
    /// <param name="serverBackupPath">Server-visible dedicated backup file.</param>
    /// <param name="targetDatabaseName">New destination database name.</param>
    /// <param name="expectedBackup">Previously retained source and media identity.</param>
    /// <param name="fileDestinations">One server-visible absolute path per logical file.</param>
    /// <param name="commandTimeoutSeconds">Timeout for each command.</param>
    /// <param name="cancellationToken">Caller cancellation.</param>
    /// <returns>An immutable plan after native checksum and MOVE verification.</returns>
    /// <remarks>
    /// SQL Server verifies readability, checksums and relocation using MOVE. Native free-space verification
    /// varies by server and platform; a successful plan does not establish sufficient physical capacity.
    /// Independently confirm destination capacity. This preflight reserves neither names, media, access nor space,
    /// and restore revalidates it. File sizes exclude CHECKDB, growth and other temporary space. The caller owns the target
    /// database and its explicit cleanup after restore; preparation never creates or deletes it.
    /// </remarks>
    public virtual async Task<SqlServerRestorePlan> PrepareRestoreAsNewAsync(
        string connectionString, string serverBackupPath, string targetDatabaseName,
        SqlServerBackupIdentity expectedBackup, IReadOnlyDictionary<string, string> fileDestinations,
        int commandTimeoutSeconds = 3600, CancellationToken cancellationToken = default)
    {
        ValidateDiskBackupPath(connectionString, serverBackupPath, commandTimeoutSeconds);
        if (string.IsNullOrWhiteSpace(targetDatabaseName) || targetDatabaseName.Length > 128 || targetDatabaseName.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("A new database name of at most 128 characters is required.", nameof(targetDatabaseName));
        }

        if (expectedBackup is null)
        {
            throw new ArgumentNullException(nameof(expectedBackup));
        }

        if (fileDestinations is null)
        {
            throw new ArgumentNullException(nameof(fileDestinations));
        }

        fileDestinations = SnapshotRestoreDestinations(fileDestinations);
        SqlServerDiskBackupHeader header = await ReadDiskBackupHeaderAsync(
            connectionString, serverBackupPath, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        DemandExpectedFullBackup(header, expectedBackup);

        IReadOnlyList<SqlServerBackupFileInfo> files = await ReadDiskBackupFileListAsync(
            connectionString, serverBackupPath, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        string moveClauses = BuildRestoreMoves(files, fileDestinations);
        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken).ConfigureAwait(false);
        await DemandRestoreCreationPermissionAsync(connection, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        await DemandNewDatabaseNameAsync(connection, targetDatabaseName, commandTimeoutSeconds,
            cancellationToken).ConfigureAwait(false);
        await DemandUnassignedRestorePathsAsync(connection, fileDestinations.Values, commandTimeoutSeconds,
            cancellationToken).ConfigureAwait(false);
        using (var verify = new SqlCommand(
            "RESTORE VERIFYONLY FROM DISK = @backupPath WITH FILE = 1, MEDIANAME = @mediaName, "
                + "CHECKSUM, STOP_ON_ERROR, "
                + moveClauses, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            verify.Parameters.Add("@backupPath", SqlDbType.NVarChar, 4000).Value = serverBackupPath;
            verify.Parameters.Add("@mediaName", SqlDbType.NVarChar, 128).Value = expectedBackup.MediaName;
            await ExecuteRecoveryNonQueryAsync(verify, cancellationToken).ConfigureAwait(false);
        }

        SqlServerDiskBackupHeader currentHeader = await ReadDiskBackupHeaderAsync(
            connectionString, serverBackupPath, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        DemandExpectedFullBackup(currentHeader, expectedBackup);
        return new SqlServerRestorePlan(serverBackupPath, targetDatabaseName, currentHeader, files, fileDestinations);
    }

    private async Task DemandRestoreCreationPermissionAsync(SqlConnection connection, int timeout, CancellationToken token)
    {
        using var command = new SqlCommand(
            "SELECT CASE WHEN IS_SRVROLEMEMBER('sysadmin') = 1 OR IS_SRVROLEMEMBER('dbcreator') = 1 "
            + "OR HAS_PERMS_BY_NAME(NULL, 'SERVER', 'CREATE ANY DATABASE') = 1 "
            + "OR HAS_PERMS_BY_NAME('master', 'DATABASE', 'CREATE DATABASE') = 1 THEN 1 ELSE 0 END", connection)
        { CommandTimeout = timeout };
        if (Convert.ToInt32(await ExecuteRecoveryScalarAsync(command, token).ConfigureAwait(false)) != 1)
            throw new UnauthorizedAccessException("Restoring a new database requires database-creation permission.");
    }
}
