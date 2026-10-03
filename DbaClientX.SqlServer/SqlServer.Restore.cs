using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Reads the logical files in the first backup set of a SQL Server-visible disk file.</summary>
    /// <param name="connectionString">Connection to the SQL Server instance that can read the backup.</param>
    /// <param name="serverBackupPath">Backup path as seen by the SQL Server service account.</param>
    /// <param name="commandTimeoutSeconds">Metadata command timeout.</param>
    /// <param name="cancellationToken">Cancellation requested by the caller.</param>
    /// <returns>Logical names, original physical paths, and file types needed to plan relocation.</returns>
    public virtual async Task<IReadOnlyList<SqlServerBackupFileInfo>> ReadDiskBackupFileListAsync(
        string connectionString,
        string serverBackupPath,
        int commandTimeoutSeconds = 3600,
        CancellationToken cancellationToken = default)
    {
        ValidateDiskBackupPath(connectionString, serverBackupPath, commandTimeoutSeconds);
        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken).ConfigureAwait(false);
        using var command = new SqlCommand(
            "RESTORE FILELISTONLY FROM DISK = @backupPath WITH FILE = 1", connection)
        { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add("@backupPath", SqlDbType.NVarChar, 4000).Value = serverBackupPath;
        using var reader = await ExecuteRecoveryReaderAsync(command, cancellationToken).ConfigureAwait(false);
        var files = new List<SqlServerBackupFileInfo>();
        while (await ReadRecoveryRowAsync(reader, command.CommandText, cancellationToken).ConfigureAwait(false))
        {
            files.Add(new SqlServerBackupFileInfo(
                reader.GetString(reader.GetOrdinal("LogicalName")),
                reader.GetString(reader.GetOrdinal("PhysicalName")),
                reader.GetString(reader.GetOrdinal("Type")),
                Convert.ToInt64(reader["Size"]),
                reader["UniqueID"] is DBNull ? null : (Guid)reader["UniqueID"],
                reader["FileId"] is DBNull ? null : Convert.ToInt64(reader["FileId"])));
        }

        if (files.Count == 0)
        {
            throw new InvalidOperationException("The backup contains no database files.");
        }

        return files.AsReadOnly();
    }

    /// <summary>
    /// Restores an expected backup set to a new database name and explicitly relocated files.
    /// </summary>
    /// <param name="connectionString">Connection to the destination SQL Server instance.</param>
    /// <param name="serverBackupPath">Backup path as seen by the destination SQL Server service account.</param>
    /// <param name="targetDatabaseName">New database name; an existing database is refused.</param>
    /// <param name="expectedBackup">Source, backup-set, and media identity retained when the backup was created.</param>
    /// <param name="fileDestinations">One SQL Server-visible absolute destination path per logical backup file.</param>
    /// <param name="commandTimeoutSeconds">Timeout for each verification and restore command.</param>
    /// <param name="cancellationToken">Cancellation requested by the caller.</param>
    /// <remarks>
    /// This method never uses REPLACE. It checks the pinned backup identity and asks SQL Server to match the media
    /// name during the restore command. Callers must still choose new physical paths and protect backup media from
    /// modification; a media name is not an authenticity signature. This does not cut over an application or
    /// validate its archive. Application locks coordinate library callers restoring the same target. Administrators
    /// must keep other tooling from modifying the target name, destination files or backup media during the operation.
    /// </remarks>
    public virtual async Task RestoreDatabaseAsNewAsync(
        string connectionString,
        string serverBackupPath,
        string targetDatabaseName,
        SqlServerBackupIdentity expectedBackup,
        IReadOnlyDictionary<string, string> fileDestinations,
        int commandTimeoutSeconds = 3600,
        CancellationToken cancellationToken = default)
    {
        var plan = await PrepareRestoreAsNewAsync(connectionString, serverBackupPath, targetDatabaseName,
            expectedBackup, fileDestinations, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        await ExecutePreparedRestoreAsync(connectionString, plan, commandTimeoutSeconds, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>Revalidates a prepared plan and restores its pinned backup to a new database.</summary>
    /// <remarks>
    /// Preparation does not reserve a name or disk space. This overload repeats preparation before restoring.
    /// The restored database is retained for caller-owned integrity/application checks and explicit cleanup.
    /// A failed or cancelled restore can leave a database in RESTORING state; inspect it before cleanup.
    /// </remarks>
    /// <param name="connectionString">Connection to the destination instance.</param>
    /// <param name="plan">Immutable preflight result to revalidate.</param>
    /// <param name="commandTimeoutSeconds">Timeout for each command.</param>
    /// <param name="cancellationToken">Caller cancellation.</param>
    public virtual Task RestoreDatabaseAsNewAsync(string connectionString, SqlServerRestorePlan plan,
        int commandTimeoutSeconds = 3600, CancellationToken cancellationToken = default)
    {
        if (plan is null) throw new ArgumentNullException(nameof(plan));
        return RestoreDatabaseAsNewAsync(connectionString, plan.ServerBackupPath, plan.TargetDatabaseName,
            plan.Header.Identity, plan.FileDestinations, commandTimeoutSeconds, cancellationToken);
    }

    private async Task ExecutePreparedRestoreAsync(string connectionString, SqlServerRestorePlan plan,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        // A physical connection owns the application lock. Disposing it must release the lock even after cancellation.
        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken, pooling: false)
            .ConfigureAwait(false);
        await AcquireRestoreTargetLockAsync(connection, plan.TargetDatabaseName, commandTimeoutSeconds,
            cancellationToken).ConfigureAwait(false);
        await DemandNewDatabaseNameAsync(connection, plan.TargetDatabaseName, commandTimeoutSeconds,
            cancellationToken).ConfigureAwait(false);
        await DemandUnassignedRestorePathsAsync(connection, plan.FileDestinations.Values, commandTimeoutSeconds,
            cancellationToken).ConfigureAwait(false);
        SqlServerDiskBackupHeader currentHeader = await ReadDiskBackupHeaderAsync(
            connectionString, plan.ServerBackupPath, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        DemandExpectedFullBackup(currentHeader, plan.Header.Identity);
        string quotedName = "[" + plan.TargetDatabaseName.Replace("]", "]]") + "]";
        using var restore = new SqlCommand(
            "IF DB_ID(@targetDatabaseName) IS NOT NULL "
                + "THROW 50001, 'The restore target database already exists.', 1; "
                + "RESTORE DATABASE " + quotedName + " FROM DISK = @backupPath "
                + "WITH FILE = 1, MEDIANAME = @mediaName, RECOVERY, CHECKSUM, STOP_ON_ERROR, "
                + BuildRestoreMoves(plan.Files, plan.FileDestinations),
            connection) { CommandTimeout = commandTimeoutSeconds };
        restore.Parameters.Add("@targetDatabaseName", SqlDbType.NVarChar, 128).Value = plan.TargetDatabaseName;
        restore.Parameters.Add("@backupPath", SqlDbType.NVarChar, 4000).Value = plan.ServerBackupPath;
        restore.Parameters.Add("@mediaName", SqlDbType.NVarChar, 128).Value = plan.Header.MediaName;
        await ExecuteRecoveryNonQueryAsync(restore, cancellationToken).ConfigureAwait(false);
    }

    private static void DemandExpectedFullBackup(SqlServerDiskBackupHeader header, SqlServerBackupIdentity expectedBackup)
    {
        if (!MatchesExpectedBackupSet(header, expectedBackup))
            throw new InvalidDataException("The disk backup does not match the expected source database and backup set.");
        if (header.BackupType != 1 || !header.HasBackupChecksums || header.IsDamaged)
            throw new InvalidDataException("A complete, checksummed, undamaged database backup is required.");
    }

    private async Task AcquireRestoreTargetLockAsync(SqlConnection connection, string targetDatabaseName,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        // CHECKSUM uses master's collation, so equivalent identifiers share a resource. Collisions only serialize callers.
        using var command = new SqlCommand(
            "DECLARE @resource nvarchar(255) = N'DbaClientX.restore.' + CONVERT(nvarchar(20), CHECKSUM(@databaseName)); "
            + "DECLARE @result int; EXEC @result = sys.sp_getapplock @Resource = @resource, "
            + "@LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = @timeout; SELECT @result;", connection)
        { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add("@databaseName", SqlDbType.NVarChar, 128).Value = targetDatabaseName;
        command.Parameters.Add("@timeout", SqlDbType.Int).Value = (int)Math.Min(int.MaxValue, (long)commandTimeoutSeconds * 1000);
        if (Convert.ToInt32(await ExecuteRecoveryScalarAsync(command, cancellationToken).ConfigureAwait(false)) < 0)
            throw new InvalidOperationException("The restore target could not be reserved for this operation.");
    }

    private static bool MatchesExpectedBackupSet(SqlServerDiskBackupHeader header,
        SqlServerBackupIdentity expectedBackup)
        => string.Equals(header.DatabaseName, expectedBackup.SourceDatabaseName, StringComparison.OrdinalIgnoreCase) &&
            header.BackupSetGuid == expectedBackup.BackupSetGuid &&
            string.Equals(header.MediaName, expectedBackup.MediaName, StringComparison.Ordinal) &&
            header.MediaSetId == expectedBackup.MediaSetId;

    private static string BuildRestoreMoves(
        IReadOnlyList<SqlServerBackupFileInfo> files,
        IReadOnlyDictionary<string, string> fileDestinations)
    {
        var requested = SnapshotRestoreDestinations(fileDestinations);
        if (requested.Count != files.Count)
            throw new ArgumentException("Every backup file must have exactly one relocation target.", nameof(fileDestinations));
        var moves = new StringBuilder();
        foreach (SqlServerBackupFileInfo file in files)
        {
            if (!requested.TryGetValue(file.LogicalName, out string? path))
                throw new ArgumentException("Every backup file must have exactly one relocation target.", nameof(fileDestinations));
            if (moves.Length != 0) moves.Append(", ");
            moves.Append("MOVE N'").Append(EscapeSqlString(file.LogicalName))
                .Append("' TO N'").Append(EscapeSqlString(path)).Append("'");
        }
        return moves.ToString();
    }

    private static Dictionary<string, string> SnapshotRestoreDestinations(IReadOnlyDictionary<string, string> fileDestinations)
    {
        var requested = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var physicalPaths = new HashSet<string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> destination in fileDestinations)
        {
            if (string.IsNullOrWhiteSpace(destination.Key) || destination.Key.Length > 128 || destination.Key.IndexOf('\0') >= 0
                || string.IsNullOrWhiteSpace(destination.Value)
                || destination.Value.IndexOf('\0') >= 0 || destination.Value.Length > 4000
                || !IsServerAbsolutePath(destination.Value)
                || HasRelativePathSegments(destination.Value))
                throw new ArgumentException("Each logical file needs an unambiguous SQL Server-visible absolute destination path.",
                    nameof(fileDestinations));
            if (requested.ContainsKey(destination.Key) || !physicalPaths.Add(NormalizeRestorePath(destination.Value)))
                throw new ArgumentException("Logical names and destination paths must be unique.", nameof(fileDestinations));
            requested.Add(destination.Key, destination.Value);
        }
        if (requested.Count == 0)
            throw new ArgumentException("At least one relocation target is required.", nameof(fileDestinations));
        return requested;
    }

    private static bool HasRelativePathSegments(string path)
        => path.Split(IsWindowsRestorePath(path) ? new[] { '/', '\\' } : new[] { '/' })
            .Any(segment => segment == "." || segment == "..");

    private static bool IsWindowsRestorePath(string path)
        => path.StartsWith("\\\\", StringComparison.Ordinal)
            || (path.Length >= 3 && char.IsLetter(path[0]) && path[1] == ':');

    private static bool IsServerAbsolutePath(string path)
        => path.StartsWith("/", StringComparison.Ordinal)
            || path.StartsWith("\\\\", StringComparison.Ordinal)
            || (path.Length >= 3 && char.IsLetter(path[0]) && path[1] == ':'
                && (path[2] == '\\' || path[2] == '/'));

    private static string EscapeSqlString(string value) => value.Replace("'", "''");

    private async Task DemandNewDatabaseNameAsync(
        SqlConnection connection, string targetDatabaseName, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        using var command = new SqlCommand("SELECT DB_ID(@targetDatabaseName)", connection)
        { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add("@targetDatabaseName", SqlDbType.NVarChar, 128).Value = targetDatabaseName;
        if (await ExecuteRecoveryScalarAsync(command, cancellationToken).ConfigureAwait(false) is not DBNull)
            throw new InvalidOperationException("The restore target database already exists.");
    }

    private async Task DemandUnassignedRestorePathsAsync(
        SqlConnection connection, IEnumerable<string> destinations, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var targetPaths = new HashSet<string>(destinations.Select(NormalizeRestorePath), StringComparer.Ordinal);
        using var command = new SqlCommand("SELECT physical_name FROM sys.master_files", connection)
        { CommandTimeout = commandTimeoutSeconds };
        using var reader = await ExecuteRecoveryReaderAsync(command, cancellationToken).ConfigureAwait(false);
        while (await ReadRecoveryRowAsync(reader, command.CommandText, cancellationToken).ConfigureAwait(false))
            if (targetPaths.Contains(NormalizeRestorePath(reader.GetString(0))))
                throw new InvalidOperationException("A restore destination is already assigned to a SQL Server database.");
    }

    private static string NormalizeRestorePath(string path)
        => IsWindowsRestorePath(path) ? path.Replace('/', '\\').TrimEnd('\\').ToUpperInvariant() : path.TrimEnd('/');

    private static void ValidateDiskBackupPath(string connectionString, string serverBackupPath, int timeout)
    {
        ValidateConnectionString(connectionString);
        if (string.IsNullOrWhiteSpace(serverBackupPath) || serverBackupPath.IndexOf('\0') >= 0 || serverBackupPath.Length > 4000)
            throw new ArgumentException("A SQL Server-visible backup path of at most 4000 characters is required.", nameof(serverBackupPath));
        ValidateBackupTimeout(timeout);
    }

}
