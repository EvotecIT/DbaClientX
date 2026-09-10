using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    internal static void ValidateAutoCreateDestinationName(string destinationTable, SqlServerBulkInsertOptions? options)
    {
        if (options?.AutoCreateTable == true) _ = SqlServerDestinationTable.Parse(destinationTable);
    }

    internal static void ValidateBulkInsertSettings(int? batchSize, int? bulkCopyTimeout, SqlServerBulkInsertOptions? options)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        if (bulkCopyTimeout < 0) throw new ArgumentOutOfRangeException(nameof(bulkCopyTimeout), "Bulk copy timeout cannot be negative.");
        if (options?.NotifyAfter <= 0) throw new ArgumentOutOfRangeException(nameof(SqlServerBulkInsertOptions.NotifyAfter), "NotifyAfter must be greater than zero.");
        const SqlBulkCopyOptions supported = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.TableLock |
            SqlBulkCopyOptions.KeepNulls | SqlBulkCopyOptions.FireTriggers | SqlBulkCopyOptions.UseInternalTransaction | SqlBulkCopyOptions.AllowEncryptedValueModifications;
        if (((options?.BulkCopyOptions ?? SqlBulkCopyOptions.Default) & ~supported) != 0)
            throw new ArgumentOutOfRangeException(nameof(SqlServerBulkInsertOptions.BulkCopyOptions), "Bulk copy options contain unsupported flags.");
    }
}
