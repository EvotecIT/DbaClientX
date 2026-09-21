using System.Data;
using Npgsql;

namespace DBAClientX;

public partial class PostgreSql
{
    internal Task WriteTableCopyRowsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DataTable table,
        string destinationTable,
        int? bulkCopyTimeout,
        CancellationToken cancellationToken)
        => WriteTableAsync(
            connection,
            table,
            destinationTable,
            bulkCopyTimeout,
            transaction,
            cancellationToken);
}
