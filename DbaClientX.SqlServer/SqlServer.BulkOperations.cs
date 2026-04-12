using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Uses <see cref="SqlBulkCopy"/> to insert data contained in <paramref name="table"/> into the specified destination table.
    /// </summary>
    public virtual void BulkInsert(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        string? username = null,
        string? password = null)
    {
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        BulkInsert(connectionString, table, destinationTable, useTransaction, batchSize, bulkCopyTimeout);
    }

    /// <summary>
    /// Uses <see cref="SqlBulkCopy"/> to insert data using a full SQL Server connection string.
    /// </summary>
    public virtual void BulkInsert(
        string connectionString,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null)
    {
        ValidateConnectionString(connectionString);
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            using var bulkCopy = CreateBulkCopy(connection!, transaction);
            ConfigureBulkCopy(bulkCopy, table, destinationTable, batchSize, bulkCopyTimeout);
            WriteToServer(bulkCopy, table);
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
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to insert data contained in <paramref name="table"/> into the specified destination table.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
    {
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        await BulkInsertAsync(connectionString, table, destinationTable, useTransaction, batchSize, bulkCopyTimeout, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to insert data using a full SQL Server connection string.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string connectionString,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;

        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            using var bulkCopy = CreateBulkCopy(connection!, transaction);
            ConfigureBulkCopy(bulkCopy, table, destinationTable, batchSize, bulkCopyTimeout);
            await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
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
    /// Creates a configured <see cref="SqlBulkCopy"/> instance for the supplied connection and transaction.
    /// </summary>
    /// <param name="connection">An open SQL Server connection.</param>
    /// <param name="transaction">Optional transaction to enlist the bulk copy operation in.</param>
    /// <returns>A configured <see cref="SqlBulkCopy"/> instance.</returns>
    protected virtual SqlBulkCopy CreateBulkCopy(SqlConnection connection, SqlTransaction? transaction) => new(connection, SqlBulkCopyOptions.Default, transaction);

    /// <summary>
    /// Writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    /// <param name="bulkCopy">The configured bulk copy instance.</param>
    /// <param name="table">Source data table.</param>
    protected virtual void WriteToServer(SqlBulkCopy bulkCopy, DataTable table) => bulkCopy.WriteToServer(table);

    /// <summary>
    /// Asynchronously writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    /// <param name="bulkCopy">The configured bulk copy instance.</param>
    /// <param name="table">Source data table.</param>
    /// <param name="cancellationToken">Token used to cancel the bulk copy operation.</param>
    /// <returns>A task that completes when the transfer finishes.</returns>
    protected virtual Task WriteToServerAsync(SqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken) => bulkCopy.WriteToServerAsync(table, cancellationToken);

    private static void ConfigureBulkCopy(SqlBulkCopy bulkCopy, DataTable table, string destinationTable, int? batchSize, int? bulkCopyTimeout)
    {
        bulkCopy.DestinationTableName = destinationTable;
        if (batchSize.HasValue)
        {
            bulkCopy.BatchSize = batchSize.Value;
        }
        if (bulkCopyTimeout.HasValue)
        {
            bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
        }

        foreach (DataColumn column in table.Columns)
        {
            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
        }
    }

    /// <summary>
    /// Creates a new <see cref="SqlConnection"/> for the supplied connection string.
    /// </summary>
    /// <param name="connectionString">Connection string used to construct the connection.</param>
    /// <returns>An unopened <see cref="SqlConnection"/> instance.</returns>
    protected virtual SqlConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens the provided <see cref="SqlConnection"/>.
    /// </summary>
    /// <param name="connection">Connection to open.</param>
    protected virtual void OpenConnection(SqlConnection connection) => connection.Open();

    /// <summary>
    /// Asynchronously opens the provided <see cref="SqlConnection"/>.
    /// </summary>
    /// <param name="connection">Connection to open.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>A task that completes when the connection is open.</returns>
    protected virtual Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);

    /// <summary>
    /// Disposes a SQL Server connection created for the current operation.
    /// </summary>
    protected virtual void DisposeConnection(SqlConnection connection) => connection.Dispose();

    /// <summary>
    /// Asynchronously disposes a SQL Server connection created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeConnectionAsync(SqlConnection connection)
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
