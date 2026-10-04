using System.IO;

namespace DBAClientX;

public partial class SqlServer
{
    // Native restore is still authoritative. This bounded validator refuses ambiguous or unsupported histories
    // before target creation, while accepting log ranges that overlap the selected data backup's ending LSN.
    internal static void ValidateRestoreChain(IReadOnlyList<SqlServerRestoreChainStep> steps)
    {
        if (steps.Count == 0 || steps[0].Header.BackupType != 1)
            throw new InvalidDataException("A restore chain must begin with a full database backup.");
        var first = steps[0].Header;
        var firstSequence = first.ChainMetadata ?? throw new InvalidDataException("Native chain metadata is required.");
        Guid? family = first.FamilyGuid;
        Guid? fork = firstSequence.RecoveryForkId;
        if (!family.HasValue || family == Guid.Empty || !fork.HasValue || fork == Guid.Empty)
            throw new InvalidDataException("Database family and recovery-fork identities are required.");
        decimal cursor = 0;
        var identities = new HashSet<Guid>();
        for (int index = 0; index < steps.Count; index++)
        {
            var header = steps[index].Header;
            var sequence = header.ChainMetadata ?? throw new InvalidDataException("Native chain metadata is required.");
            if (!MatchesExpectedBackupSet(header, steps[index].Source.ExpectedBackup) || !identities.Add(header.BackupSetGuid))
                throw new InvalidDataException("Every chain step must match a distinct pinned backup identity.");
            if (!header.HasBackupChecksums || header.IsDamaged || sequence.IsSnapshot || sequence.HasIncompleteMetadata)
                throw new InvalidDataException("Complete, checksummed, undamaged conventional disk backups are required.");
            if (header.FamilyGuid != family || !string.Equals(header.DatabaseName, first.DatabaseName, StringComparison.OrdinalIgnoreCase)
                || sequence.FirstRecoveryForkId != fork || sequence.RecoveryForkId != fork || sequence.ForkPointLsn.HasValue)
                throw new InvalidDataException("Only one database family and a single recovery fork are supported.");
            if (!sequence.FirstLsn.HasValue || !sequence.LastLsn.HasValue || sequence.FirstLsn <= 0
                || sequence.LastLsn <= sequence.FirstLsn)
                throw new InvalidDataException("A complete increasing native backup LSN range is required.");
            if (index != 0)
            {
                if (header.BackupType == 5)
                {
                    // The backup-set GUID is the authoritative base identity. Do not compare the base's
                    // FirstLSN to CheckpointLSN: an active transaction can make those different.
                    if (index != 1 || first.IsCopyOnly || header.IsCopyOnly
                        || sequence.DifferentialBaseGuid != first.BackupSetGuid
                        || !sequence.DifferentialBaseLsn.HasValue || sequence.DifferentialBaseLsn <= 0
                        || sequence.LastLsn <= cursor)
                        throw new InvalidDataException("The optional differential must follow its conventional full base.");
                }
                else if (header.BackupType != 2 || sequence.FirstLsn > cursor || sequence.LastLsn <= cursor)
                    throw new InvalidDataException("Log backups must cover and advance the preceding ending LSN without gaps.");
            }
            cursor = sequence.LastLsn.Value;
        }
    }
}
