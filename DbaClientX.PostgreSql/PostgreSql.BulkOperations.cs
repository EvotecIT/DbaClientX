using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>
    /// Performs a bulk insert into a PostgreSQL destination table using the <c>COPY</c> protocol where possible.
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
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
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

                    WriteTable(connection, batchTable, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null);
                }
            }
            else
            {
                WriteTable(connection, table, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null);
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
    /// Asynchronously performs a bulk insert into a PostgreSQL destination table using the <c>COPY</c> protocol where possible.
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
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
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

                    await WriteTableAsync(connection, batchTable, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                await WriteTableAsync(connection, table, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null, cancellationToken).ConfigureAwait(false);
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

    protected virtual void WriteTable(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction)
    {
        var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
        var copyCommand = $"COPY {destinationTable} ({columns}) FROM STDIN (FORMAT BINARY)";
        using var importer = connection.BeginBinaryImport(copyCommand);
        if (bulkCopyTimeout.HasValue)
        {
            importer.Timeout = TimeSpan.FromSeconds(bulkCopyTimeout.Value);
        }

        foreach (DataRow row in table.Rows)
        {
            importer.StartRow();
            foreach (DataColumn column in table.Columns)
            {
                var value = row[column];
                if (value == null || value == DBNull.Value)
                {
                    importer.WriteNull();
                }
                else
                {
                    importer.Write((dynamic)value!);
                }
            }
        }

        importer.Complete();
    }

    protected virtual async Task WriteTableAsync(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
        var copyCommand = $"COPY {destinationTable} ({columns}) FROM STDIN (FORMAT BINARY)";
        await using var importer = await connection.BeginBinaryImportAsync(copyCommand, cancellationToken).ConfigureAwait(false);
        if (bulkCopyTimeout.HasValue)
        {
            importer.Timeout = TimeSpan.FromSeconds(bulkCopyTimeout.Value);
        }

        foreach (DataRow row in table.Rows)
        {
            await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);
            foreach (DataColumn column in table.Columns)
            {
                var value = row[column];
                if (value == null || value == DBNull.Value)
                {
                    await importer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await importer.WriteAsync((dynamic)value!, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            }
        }

        await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    protected virtual NpgsqlConnection CreateConnection(string connectionString) => new(connectionString);

    protected virtual void OpenConnection(NpgsqlConnection connection) => connection.Open();

    protected virtual Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);
}
