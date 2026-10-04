using System.Data;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Revalidates and restores a pinned chain to a new database, then performs explicit final recovery.</summary>
    /// <param name="connectionString">Destination-instance connection.</param>
    /// <param name="plan">Prepared immutable chain with explicit relocation.</param>
    /// <param name="commandTimeoutSeconds">Positive timeout for each preparation/restore command.</param>
    /// <param name="cancellationToken">Caller cancellation.</param>
    /// <remarks>Never uses REPLACE or command replay. One non-pooled connection holds the target application lock
    /// across every NORECOVERY step and final RECOVERY. Other tools/administrators must protect target/media/files.
    /// A failed or cancelled operation can leave an owned RESTORING database; it is retained for explicit inspection
    /// and cleanup. Successful recovery still requires CHECKDB and application-specific readiness checks.</remarks>
    public virtual async Task RestoreChainAsNewAsync(string connectionString, SqlServerRestoreChainPlan plan,
        int commandTimeoutSeconds = 3600, CancellationToken cancellationToken = default)
    {
        if (plan is null) throw new ArgumentNullException(nameof(plan));
        var current = await PrepareRestoreChainAsNewAsync(connectionString, plan.Steps.Select(step => step.Source).ToArray(),
            plan.TargetDatabaseName, plan.FileDestinations, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        using var connection = await OpenRecoveryConnectionAsync(connectionString, cancellationToken, pooling: false).ConfigureAwait(false);
        await AcquireRestoreTargetLockAsync(connection, current.TargetDatabaseName, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        await DemandNewDatabaseNameAsync(connection, current.TargetDatabaseName, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        await DemandUnassignedRestorePathsAsync(connection, current.FileDestinations.Values, commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
        // Recheck every identity/range under the shared target lock before the first destructive restore command.
        var steps = new List<SqlServerRestoreChainStep>();
        foreach (var step in current.Steps)
        {
            var header = await ReadDiskBackupHeaderAsync(connectionString, step.Source.ServerBackupPath,
                commandTimeoutSeconds, cancellationToken).ConfigureAwait(false);
            steps.Add(new SqlServerRestoreChainStep(step.Source, header));
        }
        ValidateRestoreChain(steps);
        string name = "[" + current.TargetDatabaseName.Replace("]", "]]") + "]";
        for (int index = 0; index < steps.Count; index++)
        {
            var step = steps[index];
            string verb = step.Header.BackupType == 2 ? "RESTORE LOG " : "RESTORE DATABASE ";
            string firstGuard = index == 0 ? "IF DB_ID(@target) IS NOT NULL THROW 50001, 'The restore target database already exists.', 1; " : "";
            using var restore = new SqlCommand(firstGuard + verb + name
                + " FROM DISK=@path WITH FILE=1, MEDIANAME=@media, CHECKSUM, STOP_ON_ERROR, NORECOVERY"
                + (index == 0 ? ", " + BuildRestoreMoves(current.BasePlan.Files, current.FileDestinations) : ""), connection)
            { CommandTimeout = commandTimeoutSeconds };
            restore.Parameters.Add("@target", SqlDbType.NVarChar, 128).Value = current.TargetDatabaseName;
            restore.Parameters.Add("@path", SqlDbType.NVarChar, 4000).Value = step.Source.ServerBackupPath;
            restore.Parameters.Add("@media", SqlDbType.NVarChar, 128).Value = step.Header.MediaName;
            await ExecuteRecoveryNonQueryAsync(restore, cancellationToken).ConfigureAwait(false);
        }
        using var recover = new SqlCommand("RESTORE DATABASE " + name + " WITH RECOVERY", connection) { CommandTimeout = commandTimeoutSeconds };
        await ExecuteRecoveryNonQueryAsync(recover, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
    }
}
