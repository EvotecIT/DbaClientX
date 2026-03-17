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
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    WriteRows(connection, EnumerateRows(table.Rows, offset, batchSize.Value), table.Columns, destinationTable, bulkCopyTimeout, transaction);
                }
            }
            else
            {
                WriteTable(connection, table, destinationTable, bulkCopyTimeout, transaction);
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
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (var offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    await WriteRowsAsync(connection, EnumerateRows(table.Rows, offset, batchSize.Value), table.Columns, destinationTable, bulkCopyTimeout, transaction, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                await WriteTableAsync(connection, table, destinationTable, bulkCopyTimeout, transaction, cancellationToken).ConfigureAwait(false);
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
    /// Writes all rows from <paramref name="table"/> into <paramref name="destinationTable"/> using PostgreSQL binary COPY.
    /// </summary>
    /// <param name="connection">An open PostgreSQL connection.</param>
    /// <param name="table">Source table whose rows will be copied.</param>
    /// <param name="destinationTable">Fully qualified destination table name.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (seconds) applied to the COPY operation.</param>
    /// <param name="transaction">Ambient transaction to enlist in, if any.</param>
    protected virtual void WriteTable(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction)
        => WriteRows(connection, table.Rows.Cast<DataRow>(), table.Columns, destinationTable, bulkCopyTimeout, transaction);

    /// <summary>
    /// Writes the provided row sequence into <paramref name="destinationTable"/> using PostgreSQL binary COPY.
    /// </summary>
    protected virtual void WriteRows(NpgsqlConnection connection, IEnumerable<DataRow> rows, DataColumnCollection columns, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction)
    {
        var copyCommand = BuildCopyCommand(columns, destinationTable);
        using var importer = connection.BeginBinaryImport(copyCommand);
        if (bulkCopyTimeout.HasValue)
        {
            importer.Timeout = TimeSpan.FromSeconds(bulkCopyTimeout.Value);
        }

        foreach (var row in rows)
        {
            importer.StartRow();
            foreach (DataColumn column in columns)
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
        => await WriteRowsAsync(connection, table.Rows.Cast<DataRow>(), table.Columns, destinationTable, bulkCopyTimeout, transaction, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Asynchronously writes the provided row sequence into <paramref name="destinationTable"/> using PostgreSQL binary COPY.
    /// </summary>
    protected virtual async Task WriteRowsAsync(NpgsqlConnection connection, IEnumerable<DataRow> rows, DataColumnCollection columns, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var copyCommand = BuildCopyCommand(columns, destinationTable);
        await using var importer = await connection.BeginBinaryImportAsync(copyCommand, cancellationToken).ConfigureAwait(false);
        if (bulkCopyTimeout.HasValue)
        {
            importer.Timeout = TimeSpan.FromSeconds(bulkCopyTimeout.Value);
        }

        foreach (var row in rows)
        {
            await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);
            foreach (DataColumn column in columns)
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
    /// Builds the PostgreSQL <c>COPY</c> command for the supplied destination table and columns.
    /// </summary>
    protected virtual string BuildCopyCommand(DataColumnCollection columns, string destinationTable)
    {
        var columnList = string.Join(", ", columns.Cast<DataColumn>().Select(c => QuoteIdentifier(c.ColumnName)));
        return $"COPY {QuoteIdentifierPath(destinationTable)} ({columnList}) FROM STDIN (FORMAT BINARY)";
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

    /// <summary>
    /// Disposes a PostgreSQL connection created for the current operation.
    /// </summary>
    protected virtual void DisposeConnection(NpgsqlConnection connection) => connection.Dispose();

    private static IEnumerable<DataRow> EnumerateRows(DataRowCollection rows, int start, int count)
    {
        var end = Math.Min(start + count, rows.Count);
        for (var i = start; i < end; i++)
        {
            yield return rows[i];
        }
    }

    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifier));
        }

        return "\"" + identifier.Replace("\"", "\"\"") + "\"";
    }

    private static string QuoteIdentifierPath(string identifierPath)
    {
        if (string.IsNullOrWhiteSpace(identifierPath))
        {
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifierPath));
        }

        var parts = identifierPath.Split('.');
        for (var i = 0; i < parts.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(parts[i]))
            {
                throw new ArgumentException("Identifier cannot contain empty segments.", nameof(identifierPath));
            }

            parts[i] = QuoteIdentifier(parts[i].Trim());
        }

        return string.Join(".", parts);
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
