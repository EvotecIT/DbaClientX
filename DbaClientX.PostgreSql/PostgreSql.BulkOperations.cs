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

    /// <summary>
    /// Writes all rows from <paramref name="table"/> into <paramref name="destinationTable"/> using PostgreSQL binary COPY.
    /// </summary>
    /// <param name="connection">An open PostgreSQL connection.</param>
    /// <param name="table">Source table whose rows will be copied.</param>
    /// <param name="destinationTable">Fully qualified destination table name.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (seconds) applied to the COPY operation.</param>
    /// <param name="transaction">Ambient transaction to enlist in, if any.</param>
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

    /// <summary>
    /// Asynchronously writes all rows from <paramref name="table"/> into <paramref name="destinationTable"/> using PostgreSQL binary COPY.
    /// </summary>
    /// <param name="connection">An open PostgreSQL connection.</param>
    /// <param name="table">Source table whose rows will be copied.</param>
    /// <param name="destinationTable">Fully qualified destination table name.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (seconds) applied to the COPY operation.</param>
    /// <param name="transaction">Ambient transaction to enlist in, if any.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
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

    /// <summary>
    /// Creates a new <see cref="NpgsqlConnection"/> for the given connection string.
    /// </summary>
    /// <param name="connectionString">PostgreSQL connection string.</param>
    /// <returns>A new, unopened <see cref="NpgsqlConnection"/>.</returns>
    protected virtual NpgsqlConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens the specified <paramref name="connection"/>.
    /// </summary>
    /// <param name="connection">The connection to open.</param>
    protected virtual void OpenConnection(NpgsqlConnection connection) => connection.Open();

    /// <summary>
    /// Asynchronously opens the specified <paramref name="connection"/>.
    /// </summary>
    /// <param name="connection">The connection to open.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    protected virtual Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);
}
