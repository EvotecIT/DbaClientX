using System;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Reads the identity and safety metadata of a dedicated single-set disk backup.</summary>
    /// <param name="connectionString">Connection to a SQL Server instance that can read the file.</param>
    /// <param name="serverBackupPath">Backup path as seen by the SQL Server service account.</param>
    /// <param name="commandTimeoutSeconds">Metadata command timeout.</param>
    /// <param name="cancellationToken">Cancellation requested by the caller.</param>
    /// <returns>The database and media identities, backup-set identity, type, and integrity flags.</returns>
    /// <remarks>
    /// The file must contain exactly one backup set. This reads metadata; call VerifyDiskBackupAsync
    /// and perform a test restore before trusting a recovery workflow.
    /// </remarks>
    public virtual async Task<SqlServerDiskBackupHeader> ReadDiskBackupHeaderAsync(
        string connectionString,
        string serverBackupPath,
        int commandTimeoutSeconds = 3600,
        CancellationToken cancellationToken = default)
    {
        ValidateDiskBackupPath(connectionString, serverBackupPath, commandTimeoutSeconds);
        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand("RESTORE HEADERONLY FROM DISK = @backupPath", connection)
        { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add("@backupPath", SqlDbType.NVarChar, 4000).Value = serverBackupPath;
        using var reader = await ExecuteRecoveryReaderAsync(command, cancellationToken).ConfigureAwait(false);
        if (!await ReadRecoveryRowAsync(reader, command.CommandText, cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidDataException("The disk backup contains no backup set.");
        }

        string databaseName = reader.GetString(reader.GetOrdinal("DatabaseName"));
        Guid backupSetGuid = reader.GetGuid(reader.GetOrdinal("BackupSetGUID"));
        int familyIndex = reader.GetOrdinal("FamilyGUID");
        Guid? familyGuid = reader.IsDBNull(familyIndex) ? null : reader.GetGuid(familyIndex);
        int backupType = Convert.ToInt32(reader["BackupType"], CultureInfo.InvariantCulture);
        bool isCopyOnly = reader.GetBoolean(reader.GetOrdinal("IsCopyOnly"));
        bool hasBackupChecksums = reader.GetBoolean(reader.GetOrdinal("HasBackupChecksums"));
        bool isDamaged = reader.GetBoolean(reader.GetOrdinal("IsDamaged"));
        if (await ReadRecoveryRowAsync(reader, command.CommandText, cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidDataException("A dedicated disk backup must contain only one backup set.");
        }

        if (string.IsNullOrWhiteSpace(databaseName) || backupSetGuid == Guid.Empty)
        {
            throw new InvalidDataException("The disk backup has incomplete identity metadata.");
        }

        reader.Close();
        using var labelCommand = new SqlCommand("RESTORE LABELONLY FROM DISK = @backupPath", connection)
        { CommandTimeout = commandTimeoutSeconds };
        labelCommand.Parameters.Add("@backupPath", SqlDbType.NVarChar, 4000).Value = serverBackupPath;
        using var label = await ExecuteRecoveryReaderAsync(labelCommand, cancellationToken).ConfigureAwait(false);
        if (!await ReadRecoveryRowAsync(label, labelCommand.CommandText, cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidDataException("The disk backup has no media label.");
        }

        int mediaNameIndex = label.GetOrdinal("MediaName");
        string? mediaName = label.IsDBNull(mediaNameIndex) ? null : label.GetString(mediaNameIndex);
        Guid mediaSetId = label.GetGuid(label.GetOrdinal("MediaSetId"));
        int familyCount = label.GetInt32(label.GetOrdinal("FamilyCount"));
        if (mediaName is null || mediaName.Trim().Length == 0 ||
            mediaSetId == Guid.Empty || familyCount != 1 ||
            await ReadRecoveryRowAsync(label, labelCommand.CommandText, cancellationToken).ConfigureAwait(false))
        {
            throw new InvalidDataException("A dedicated disk backup requires one named media family.");
        }

        return new SqlServerDiskBackupHeader(databaseName, backupSetGuid, familyGuid,
            mediaName, mediaSetId,
            backupType, isCopyOnly, hasBackupChecksums, isDamaged);
    }
}
