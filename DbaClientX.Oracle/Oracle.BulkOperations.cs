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
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        var dispose = false;
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            connection = _transactionConnection;
        }
        else
        {
            connection = CreateConnection(connectionString);
            OpenConnection(connection);
            dispose = true;
        }

        try
        {
            using var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
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
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    var batchTable = table.Clone();
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
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
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
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        var dispose = false;
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            connection = _transactionConnection;
        }
        else
        {
            connection = CreateConnection(connectionString);
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            dispose = true;
        }

        try
        {
            using var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
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
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    var batchTable = table.Clone();
                    for (var i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }

                    await WriteToServerAsync(bulkCopy, batchTable, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
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
    {
        WriteToServer(bulkCopy, table);
        return Task.CompletedTask;
    }

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
}
