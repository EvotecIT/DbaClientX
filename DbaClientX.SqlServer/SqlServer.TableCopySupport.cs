using System.Data;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Creates a connection using the same authentication and compatibility policy as other SQL operations.</summary>
    internal SqlConnection CreateTableCopyConnection(string connectionString) => CreateConnection(connectionString);

    /// <summary>Writes a page inside a caller-owned transaction using the canonical bulk-copy implementation.</summary>
    internal async Task WriteTableCopyRowsAsync(SqlConnection connection, SqlTransaction transaction, DataTable page, string destination, SqlServerBulkInsertOptions? options, int? batchSize, int? timeout, CancellationToken cancellationToken)
    {
        ValidateBulkInsertInputs(page, destination, batchSize, timeout, options);
        ValidateCompatibility(connection.ConnectionString, options);
        using SqlBulkCopy bulk = CreateBulkCopy(connection, transaction, options);
        ConfigureBulkCopy(bulk, page, destination, batchSize, timeout, options);
        using IDataReader? converted = await CreateConvertedReaderAsync(connection, transaction, page, bulk.DestinationTableName, options, cancellationToken).ConfigureAwait(false);
        if (converted != null) await WriteToServerAsync(bulk, converted, cancellationToken).ConfigureAwait(false);
        else await WriteToServerAsync(bulk, page, cancellationToken).ConfigureAwait(false);
    }
}
