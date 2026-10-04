using System.IO;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Prepares an explicitly ordered full, optional differential, and contiguous log chain without creating a database.</summary>
    /// <param name="connectionString">Destination-instance connection.</param>
    /// <param name="backups">Ordered dedicated single-set/single-family backups with previously retained identities.</param>
    /// <param name="targetDatabaseName">New target database name.</param>
    /// <param name="fileDestinations">One explicit service-visible path per logical file.</param>
    /// <param name="commandTimeoutSeconds">Positive timeout for each metadata/verification command.</param>
    /// <param name="cancellationToken">Caller cancellation.</param>
    /// <returns>Immutable pinned steps and maximum recorded file allocation.</returns>
    /// <remarks>Supports one database family/recovery fork and an unchanged ordinary data/log file inventory.
    /// Refuses snapshots, incomplete metadata, fork transitions and file additions/removals. Each set is natively
    /// checksum-verified; the full backup receives native MOVE/capacity preflight. Later growth and CHECKDB space
    /// require a separate capacity budget. No target, file, permission, media or capacity reservation is made.</remarks>
    public virtual async Task<SqlServerRestoreChainPlan> PrepareRestoreChainAsNewAsync(string connectionString,
        IReadOnlyList<SqlServerRestoreChainSource> backups, string targetDatabaseName,
        IReadOnlyDictionary<string, string> fileDestinations, int commandTimeoutSeconds = 3600,
        CancellationToken cancellationToken = default)
    {
        if (backups is null) throw new ArgumentNullException(nameof(backups));
        var sources = backups.ToArray();
        if (sources.Length == 0 || sources.Any(source => source is null))
            throw new ArgumentException("An ordered non-empty list of pinned backups is required.", nameof(backups));
        if (fileDestinations is null) throw new ArgumentNullException(nameof(fileDestinations));
        // Snapshot caller-owned relocation before the first await, matching the ordinary restore contract.
        var destinations = SnapshotRestoreDestinations(fileDestinations);
        cancellationToken.ThrowIfCancellationRequested();
        var basePlan = await PrepareRestoreAsNewAsync(connectionString, sources[0].ServerBackupPath,
            targetDatabaseName, sources[0].ExpectedBackup, destinations, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        var steps = new List<SqlServerRestoreChainStep> { new(sources[0], basePlan.Header) };
        var required = basePlan.Files.ToDictionary(file => file.LogicalName, StringComparer.OrdinalIgnoreCase);
        if (required.Values.Any(file => (file.FileType != "D" && file.FileType != "L")
            || !file.UniqueId.HasValue || file.UniqueId == Guid.Empty || !file.FileId.HasValue || file.FileId <= 0))
            throw new InvalidDataException("Restore chains support ordinary data and log files only.");
        for (int index = 1; index < sources.Length; index++)
        {
            var source = sources[index];
            var header = await ReadDiskBackupHeaderAsync(connectionString, source.ServerBackupPath,
                commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
            steps.Add(new SqlServerRestoreChainStep(source, header));
            var files = await ReadDiskBackupFileListAsync(connectionString, source.ServerBackupPath,
                commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
            if (files.Count != required.Count || files.Select(file => file.LogicalName).Distinct(StringComparer.OrdinalIgnoreCase).Count() != files.Count)
                throw new InvalidDataException("Adding or removing files within a restore chain is unsupported.");
            foreach (var file in files)
            {
                if (!required.TryGetValue(file.LogicalName, out var prior) || file.FileType != prior.FileType
                    || file.UniqueId != prior.UniqueId || file.FileId != prior.FileId)
                    throw new InvalidDataException("The restore chain must retain its original logical file inventory/types.");
                if (file.SizeBytes > prior.SizeBytes) required[file.LogicalName] = file;
            }
        }
        ValidateRestoreChain(steps);
        for (int index = 1; index < sources.Length; index++)
            await VerifyDiskBackupAsync(connectionString, sources[index].ServerBackupPath,
                commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        return new SqlServerRestoreChainPlan(basePlan, steps, required.Values.ToArray());
    }
}
