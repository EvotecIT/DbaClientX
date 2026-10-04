namespace DBAClientX;

/// <summary>Conventional dedicated disk-backup kinds supported by the restore-chain workflow.</summary>
public enum SqlServerDiskBackupKind
{
    /// <summary>Full database backup.</summary>
    Full = 1,
    /// <summary>Transaction-log backup.</summary>
    Log = 2,
    /// <summary>Single-base database differential backup.</summary>
    Differential = 5
}

public partial class SqlServer
{
    /// <summary>Creates and verifies a checksummed conventional backup on a new dedicated disk file.</summary>
    /// <param name="connectionString">Connection to the source instance; commands use master.</param>
    /// <param name="databaseName">Source database.</param>
    /// <param name="serverBackupDirectory">Existing service-visible directory.</param>
    /// <param name="kind">Full, differential or transaction log.</param>
    /// <param name="copyOnly">Required explicit policy. Full copy-only backups do not establish a differential base;
    /// conventional full backups do. Conventional log backups can permit log truncation. Differentials require false.</param>
    /// <param name="commandTimeoutSeconds">Positive per-command timeout.</param>
    /// <param name="cancellationToken">Caller cancellation; a partial generated file can remain after failure.</param>
    /// <returns>Generated path and verified native backup identity/sequence metadata.</returns>
    /// <remarks>Always uses a random filename and named media, CHECKSUM and STOP_ON_ERROR. Never replays a command.
    /// Verification establishes readability/checksums, not a successful restore or application readiness.</remarks>
    public virtual async Task<SqlServerDiskBackupResult> BackupToDedicatedDiskAsync(string connectionString,
        string databaseName, string serverBackupDirectory, SqlServerDiskBackupKind kind, bool copyOnly,
        int commandTimeoutSeconds = 3600, CancellationToken cancellationToken = default)
    {
        ValidateBackupArguments(connectionString, databaseName, serverBackupDirectory, commandTimeoutSeconds);
        if (!Enum.IsDefined(typeof(SqlServerDiskBackupKind), kind)) throw new ArgumentOutOfRangeException(nameof(kind));
        if (kind == SqlServerDiskBackupKind.Differential && copyOnly)
            throw new ArgumentException("Differential backups cannot use COPY_ONLY.", nameof(copyOnly));
        string separator = serverBackupDirectory.Contains('\\') ? "\\" : "/";
        string path = serverBackupDirectory.TrimEnd('\\', '/') + separator + "DbaClientX-" + Guid.NewGuid().ToString("N") + ".bak";
        string media = "DbaClientX-" + Guid.NewGuid().ToString("N");
        string command = kind == SqlServerDiskBackupKind.Log ? "BACKUP LOG" : "BACKUP DATABASE";
        string options = (copyOnly ? "COPY_ONLY, " : "") + (kind == SqlServerDiskBackupKind.Differential ? "DIFFERENTIAL, " : "");
        await ExecuteBackupCommandAsync(connectionString, command + " @databaseName TO DISK = @backupPath WITH "
            + options + "FORMAT, MEDIANAME = @mediaName, CHECKSUM, STOP_ON_ERROR", databaseName, path,
            media, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        await VerifyDiskBackupAsync(connectionString, path, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        var header = await ReadDiskBackupHeaderAsync(connectionString, path, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        if (!string.Equals(header.DatabaseName, databaseName, StringComparison.OrdinalIgnoreCase)
            || !string.Equals(header.MediaName, media, StringComparison.Ordinal) || header.BackupType != (int)kind
            || header.IsCopyOnly != copyOnly || !header.HasBackupChecksums || header.IsDamaged)
            throw new InvalidOperationException("The completed backup header does not match the requested dedicated backup.");
        cancellationToken.ThrowIfCancellationRequested();
        return new SqlServerDiskBackupResult(path, header);
    }
}
