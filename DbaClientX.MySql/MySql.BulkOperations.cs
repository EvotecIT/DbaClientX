using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    /// <summary>
    /// Performs a bulk insert using <see cref="MySqlBulkCopy"/> and the provided <see cref="DataTable"/> payload.
    /// </summary>
    public virtual void BulkInsert(
        string host,
        string database,
        string username,
        string password,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null)
    {
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var bulkCopy = CreateBulkCopy(connection!, transaction);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(column.Ordinal, column.ColumnName, null));
            }

            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    WriteToServer(bulkCopy, EnumerateRows(table.Rows, offset, batchSize.Value), table.Columns.Count);
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
    /// Performs a bulk insert asynchronously using <see cref="MySqlBulkCopy"/> and the provided <see cref="DataTable"/> payload.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string host,
        string database,
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

        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var bulkCopy = CreateBulkCopy(connection!, transaction);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(column.Ordinal, column.ColumnName, null));
            }

            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    await WriteToServerAsync(bulkCopy, EnumerateRows(table.Rows, offset, batchSize.Value), table.Columns.Count, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
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
    /// Creates a configured <see cref="MySqlBulkCopy"/> instance for bulk operations.
    /// </summary>
    protected virtual MySqlBulkCopy CreateBulkCopy(MySqlConnection connection, MySqlTransaction? transaction) => new(connection, transaction);

    /// <summary>
    /// Writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual void WriteToServer(MySqlBulkCopy bulkCopy, DataTable table) => bulkCopy.WriteToServer(table);

    /// <summary>
    /// Asynchronously writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual Task WriteToServerAsync(MySqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken) => bulkCopy.WriteToServerAsync(table, cancellationToken).AsTask();

    /// <summary>
    /// Writes a row sequence to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual void WriteToServer(MySqlBulkCopy bulkCopy, IEnumerable<DataRow> rows, int columnCount) => bulkCopy.WriteToServer(rows, columnCount);

    /// <summary>
    /// Asynchronously writes a row sequence to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual Task WriteToServerAsync(MySqlBulkCopy bulkCopy, IEnumerable<DataRow> rows, int columnCount, CancellationToken cancellationToken) => bulkCopy.WriteToServerAsync(rows, columnCount, cancellationToken).AsTask();

    /// <summary>
    /// Creates a new <see cref="MySqlConnection"/> for the supplied connection string.
    /// </summary>
    protected virtual MySqlConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens a MySQL connection using synchronous APIs.
    /// </summary>
    protected virtual void OpenConnection(MySqlConnection connection) => connection.Open();

    /// <summary>
    /// Opens a MySQL connection asynchronously.
    /// </summary>
    protected virtual Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);

    /// <summary>
    /// Disposes a MySQL connection created for the current operation.
    /// </summary>
    protected virtual void DisposeConnection(MySqlConnection connection) => connection.Dispose();

    /// <summary>
    /// Asynchronously disposes a MySQL connection created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeConnectionAsync(MySqlConnection connection)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return connection.DisposeAsync();
#else
        connection.Dispose();
        return default;
#endif
    }

    private static IEnumerable<DataRow> EnumerateRows(DataRowCollection rows, int start, int count)
    {
        var end = Math.Min(start + count, rows.Count);
        for (var i = start; i < end; i++)
        {
            yield return rows[i];
        }
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
