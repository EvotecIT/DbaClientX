using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    /// <summary>
    /// Performs a bulk insert into an Oracle table using <see cref="OracleBulkCopy"/>.
    /// </summary>
    public virtual void BulkInsert(
        string host,
        string serviceName,
        string username,
        string password,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null)
    {
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            using var bulkCopy = CreateBulkCopy(connection, transaction);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                var batchTable = table.Clone();
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    batchTable.Clear();
                    for (var i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }

                    WriteToServer(bulkCopy, batchTable);
                }
            }
            else
            {
                WriteToServer(bulkCopy, table);
            }
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Asynchronously performs a bulk insert into an Oracle table using <see cref="OracleBulkCopy"/>.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string host,
        string serviceName,
        string username,
        string password,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default)
    {
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            using var bulkCopy = CreateBulkCopy(connection, transaction);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                var batchTable = table.Clone();
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    batchTable.Clear();
                    for (var i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }

                    await WriteToServerAsync(bulkCopy, batchTable, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                cancellationToken.ThrowIfCancellationRequested();
                await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Creates the <see cref="OracleBulkCopy"/> instance used by the bulk insert helpers.
    /// </summary>
    protected virtual OracleBulkCopy CreateBulkCopy(OracleConnection connection, OracleTransaction? transaction) => new(connection);

    /// <summary>
    /// Performs the synchronous write for the supplied <see cref="OracleBulkCopy"/>.
    /// </summary>
    protected virtual void WriteToServer(OracleBulkCopy bulkCopy, DataTable table) => bulkCopy.WriteToServer(table);

    /// <summary>
    /// Performs the asynchronous write for the supplied <see cref="OracleBulkCopy"/>.
    /// </summary>
    protected virtual Task WriteToServerAsync(OracleBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        => Task.Run(() =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            WriteToServer(bulkCopy, table);
        }, cancellationToken);

    /// <summary>
    /// Creates an <see cref="OracleConnection"/> for the provided connection string.
    /// </summary>
    protected virtual OracleConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens the supplied <see cref="OracleConnection"/> synchronously.
    /// </summary>
    protected virtual void OpenConnection(OracleConnection connection) => connection.Open();

    /// <summary>
    /// Opens the supplied <see cref="OracleConnection"/> asynchronously.
    /// </summary>
    protected virtual Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);

    /// <summary>
    /// Disposes an Oracle connection created for the current operation.
    /// </summary>
    protected virtual void DisposeConnection(OracleConnection connection) => connection.Dispose();

    /// <summary>
    /// Asynchronously disposes an Oracle connection created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeConnectionAsync(OracleConnection connection)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return connection.DisposeAsync();
#else
        connection.Dispose();
        return default;
#endif
    }

    private static void ValidateBulkInsertInputs(DataTable table, string destinationTable, int? batchSize, int? bulkCopyTimeout)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        if (string.IsNullOrWhiteSpace(destinationTable))
        {
            throw new ArgumentException("Destination table cannot be null or whitespace.", nameof(destinationTable));
        }

        if (table.Columns.Count == 0)
        {
            throw new ArgumentException("Bulk insert requires at least one column.", nameof(table));
        }

        if (batchSize.HasValue && batchSize.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");
        }

        if (bulkCopyTimeout.HasValue && bulkCopyTimeout.Value <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bulkCopyTimeout), "Bulk copy timeout must be greater than zero.");
        }
    }
}
