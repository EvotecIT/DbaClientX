using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public sealed partial class OracleTableCopyAdapter
{
    internal const string OracleDurableDestinationTableQuery =
        "SELECT SEGMENT_CREATED FROM ALL_TABLES WHERE OWNER = :owner AND TABLE_NAME = :table_name AND TEMPORARY = 'N'";

    private const string OracleCheckpointLeafSegmentsQuery = @"SELECT COUNT(*),
    COUNT(CASE WHEN SEGMENT_CREATED = 'YES' THEN 1 END)
FROM (
    SELECT SEGMENT_CREATED FROM ALL_TAB_PARTITIONS
    WHERE TABLE_OWNER = :owner AND TABLE_NAME = :table_name AND COMPOSITE = 'NO'
    UNION ALL
    SELECT SEGMENT_CREATED FROM ALL_TAB_SUBPARTITIONS
    WHERE TABLE_OWNER = :owner AND TABLE_NAME = :table_name
)";

    /// <summary>Validates existing storage without allocating or modifying a user destination.</summary>
    /// <remarks>Partitioned parents have no segment; only their leaf partition storage qualifies serializable writes.</remarks>
    private async Task ValidateCheckpointDestinationStorageAsync(
        OracleConnection connection, string owner, string table, string displayName,
        object? segmentCreated, CancellationToken cancellationToken)
    {
        if (string.Equals(segmentCreated as string, "YES", StringComparison.OrdinalIgnoreCase)) return;
        if (string.Equals(segmentCreated as string, "N/A", StringComparison.OrdinalIgnoreCase))
        {
            using var command = new OracleCommand(OracleCheckpointLeafSegmentsQuery, connection)
            { BindByName = true, CommandTimeout = CommandTimeout };
            command.Parameters.Add("owner", OracleDbType.Varchar2).Value = owner;
            command.Parameters.Add("table_name", OracleDbType.Varchar2).Value = table;
            using OracleDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                long total = Convert.ToInt64(reader.GetValue(0));
                long allocated = Convert.ToInt64(reader.GetValue(1));
                if (total > 0 && total == allocated) return;
            }
        }
        throw new NotSupportedException(
            $"Oracle checkpoint destination '{displayName}' requires allocated table or leaf partition storage before its serializable writes. " +
            "Create the destination with SEGMENT CREATION IMMEDIATE or allocate its storage explicitly before copying.");
    }
}
