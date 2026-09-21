using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.DataMovement;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    internal const string RollbackCapableBulkDestinationQuery = @"SELECT ENGINE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
  AND ((@@lower_case_table_names = 0 AND BINARY TABLE_SCHEMA = BINARY @database AND BINARY TABLE_NAME = BINARY @table)
       OR (@@lower_case_table_names <> 0 AND TABLE_SCHEMA = @database AND TABLE_NAME = @table))";

    private readonly ConditionalWeakTable<MySqlBulkCopy, TransactionalBulkCopyMarker> _transactionalBulkCopies = new();

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

        var connectionString = BuildBulkCopyConnectionString(host, database, username, password);
        BulkInsert(connectionString, table, destinationTable, useTransaction, batchSize, bulkCopyTimeout);
    }

    /// <summary>
    /// Performs a bulk insert using <see cref="MySqlBulkCopy"/> and a full MySQL connection string.
    /// </summary>
    public virtual void BulkInsert(
        string connectionString,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        int? bulkCopyTimeout = null)
    {
        ValidateConnectionString(connectionString, BulkCopyAllowedUnsupportedOptions);
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;
        var ownsWriteTransaction = false;

        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            EnsureRollbackCapableBulkDestination(connection!, transaction, destinationTable);
            if (transaction == null)
            {
                transaction = BeginBulkCopyTransaction(connection!);
                ownsWriteTransaction = true;
            }
            var bulkCopy = TrackBulkCopy(CreateBulkCopy(connection!, transaction), transaction);
            ConfigureBulkCopy(bulkCopy, table, destinationTable, bulkCopyTimeout);

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
            if (ownsWriteTransaction && transaction != null)
            {
                CommitDbTransaction(transaction);
            }
        }
        catch (DbaTransactionException)
        {
            if (ownsWriteTransaction) TryRollbackDbTransactionOnDispose(transaction);
            throw;
        }
        catch (Exception ex)
        {
            if (ownsWriteTransaction) TryRollbackDbTransactionOnDispose(transaction);
            throw CreateQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (ownsWriteTransaction) transaction?.Dispose();
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

        var connectionString = BuildBulkCopyConnectionString(host, database, username, password);
        await BulkInsertAsync(connectionString, table, destinationTable, useTransaction, batchSize, bulkCopyTimeout, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Performs a bulk insert asynchronously using <see cref="MySqlBulkCopy"/> and a full MySQL connection string.
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
        ValidateConnectionString(connectionString, BulkCopyAllowedUnsupportedOptions);
        ValidateBulkInsertInputs(table, destinationTable, batchSize, bulkCopyTimeout);

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;
        var ownsWriteTransaction = false;

        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            await EnsureRollbackCapableBulkDestinationAsync(
                connection!,
                transaction,
                destinationTable,
                cancellationToken).ConfigureAwait(false);
            if (transaction == null)
            {
                transaction = await BeginBulkCopyTransactionAsync(connection!, cancellationToken).ConfigureAwait(false);
                ownsWriteTransaction = true;
            }
            var bulkCopy = TrackBulkCopy(CreateBulkCopy(connection!, transaction), transaction);
            ConfigureBulkCopy(bulkCopy, table, destinationTable, bulkCopyTimeout);

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
            if (ownsWriteTransaction && transaction != null)
            {
                await CommitDbTransactionAsync(transaction, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (DbaTransactionException)
        {
            if (ownsWriteTransaction) await TryRollbackDbTransactionOnDisposeAsync(transaction).ConfigureAwait(false);
            throw;
        }
        catch (Exception ex) when (!IsCallerCancellation(ex, cancellationToken))
        {
            if (ownsWriteTransaction) await TryRollbackDbTransactionOnDisposeAsync(transaction).ConfigureAwait(false);
            throw CreateQueryExecutionOrCancellationException("Failed to execute bulk insert.", destinationTable, ex, cancellationToken);
        }
        catch
        {
            if (ownsWriteTransaction) await TryRollbackDbTransactionOnDisposeAsync(transaction).ConfigureAwait(false);
            throw;
        }
        finally
        {
            if (ownsWriteTransaction) transaction?.Dispose();
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Creates a configured <see cref="MySqlBulkCopy"/> instance for bulk operations.
    /// </summary>
    protected virtual MySqlBulkCopy CreateBulkCopy(MySqlConnection connection, MySqlTransaction? transaction) => new(connection, transaction);

    /// <summary>Begins the rollback-capable transaction used for an otherwise standalone strict bulk write.</summary>
    protected virtual MySqlTransaction? BeginBulkCopyTransaction(MySqlConnection connection)
        => BeginDbTransaction(connection, IsolationLevel.ReadCommitted);

    /// <summary>Begins the rollback-capable transaction used for an otherwise standalone strict asynchronous bulk write.</summary>
    protected virtual async Task<MySqlTransaction?> BeginBulkCopyTransactionAsync(
        MySqlConnection connection,
        CancellationToken cancellationToken)
        => await BeginDbTransactionAsync(connection, IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);

    /// <summary>Ensures a strict bulk destination can roll back warning-bearing writes.</summary>
    protected virtual void EnsureRollbackCapableBulkDestination(
        MySqlConnection connection,
        MySqlTransaction? transaction,
        string destinationTable)
    {
        (string database, string table) = ResolveBulkDestination(connection, destinationTable);
        using var command = new MySqlCommand(RollbackCapableBulkDestinationQuery, connection, transaction)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@table", table);
        ValidateRollbackCapableBulkDestination(destinationTable, Convert.ToString(command.ExecuteScalar()));
    }

    /// <summary>Ensures an asynchronous strict bulk destination can roll back warning-bearing writes.</summary>
    protected virtual async Task EnsureRollbackCapableBulkDestinationAsync(
        MySqlConnection connection,
        MySqlTransaction? transaction,
        string destinationTable,
        CancellationToken cancellationToken)
    {
        (string database, string table) = ResolveBulkDestination(connection, destinationTable);
        await using var command = new MySqlCommand(RollbackCapableBulkDestinationQuery, connection, transaction)
        {
            CommandTimeout = CommandTimeout
        };
        command.Parameters.AddWithValue("@database", database);
        command.Parameters.AddWithValue("@table", table);
        ValidateRollbackCapableBulkDestination(
            destinationTable,
            Convert.ToString(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)));
    }

    private static (string Database, string Table) ResolveBulkDestination(
        MySqlConnection connection,
        string destinationTable)
    {
        string[] segments = DbaIdentifierPath.SplitSegments(destinationTable, DbaTableCopyProvider.MySql)
            .Select(segment => DbaIdentifierPath.UnquoteSegment(segment, DbaTableCopyProvider.MySql))
            .ToArray();
        if (segments.Length is < 1 or > 2)
        {
            throw new ArgumentException(
                "MySQL bulk destinations support table or database.table names.",
                nameof(destinationTable));
        }

        string database = segments.Length == 2 ? segments[0] : connection.Database;
        if (string.IsNullOrWhiteSpace(database))
        {
            throw new InvalidOperationException(
                $"MySQL bulk destination '{destinationTable}' requires a selected database or a database-qualified table name.");
        }

        return (database, segments[segments.Length - 1]);
    }

    internal static void ValidateRollbackCapableBulkDestination(string destinationTable, string? engine)
    {
        if (string.Equals(engine, "InnoDB", StringComparison.OrdinalIgnoreCase)) return;
        throw new InvalidOperationException(
            $"MySQL bulk destination '{destinationTable}' must use InnoDB so conversion-warning failures can be rolled back; found '{engine ?? "unknown"}'.");
    }

    /// <summary>
    /// Writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual void WriteToServer(MySqlBulkCopy bulkCopy, DataTable table)
        => ThrowIfBulkCopyWarnings(bulkCopy.WriteToServer(table), bulkCopy.DestinationTableName, bulkCopy);

    /// <summary>
    /// Asynchronously writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual async Task WriteToServerAsync(MySqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken)
        => ThrowIfBulkCopyWarnings(
            await bulkCopy.WriteToServerAsync(table, cancellationToken).ConfigureAwait(false),
            bulkCopy.DestinationTableName,
            bulkCopy);

    /// <summary>
    /// Writes a row sequence to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual void WriteToServer(MySqlBulkCopy bulkCopy, IEnumerable<DataRow> rows, int columnCount)
        => ThrowIfBulkCopyWarnings(bulkCopy.WriteToServer(rows, columnCount), bulkCopy.DestinationTableName, bulkCopy);

    /// <summary>
    /// Asynchronously writes a row sequence to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual async Task WriteToServerAsync(MySqlBulkCopy bulkCopy, IEnumerable<DataRow> rows, int columnCount, CancellationToken cancellationToken)
        => ThrowIfBulkCopyWarnings(
            await bulkCopy.WriteToServerAsync(rows, columnCount, cancellationToken).ConfigureAwait(false),
            bulkCopy.DestinationTableName,
            bulkCopy);

    private static void ConfigureBulkCopy(MySqlBulkCopy bulkCopy, DataTable table, string destinationTable, int? bulkCopyTimeout)
    {
        bulkCopy.DestinationTableName = destinationTable;
        if (bulkCopyTimeout.HasValue)
        {
            bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
        }

        foreach (DataColumn column in table.Columns)
        {
            bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(column.Ordinal, column.ColumnName, null));
        }
    }

    private static string BuildBulkCopyConnectionString(string host, string database, string username, string password)
    {
        var connectionString = BuildConnectionString(host, database, username, password);
        var separator = connectionString.EndsWith(";", StringComparison.Ordinal) ? string.Empty : ";";
        return connectionString + separator + "AllowLoadLocalInfile=true";
    }

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

    private sealed class TransactionalBulkCopyMarker
    {
    }

    private MySqlBulkCopy TrackBulkCopy(MySqlBulkCopy bulkCopy, MySqlTransaction? transaction)
    {
        if (transaction != null) _transactionalBulkCopies.Add(bulkCopy, new TransactionalBulkCopyMarker());
        return bulkCopy;
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

        if (bulkCopyTimeout.HasValue && bulkCopyTimeout.Value < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bulkCopyTimeout), "Bulk copy timeout cannot be negative.");
        }
    }
}
