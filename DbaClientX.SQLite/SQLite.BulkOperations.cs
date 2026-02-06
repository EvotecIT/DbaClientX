using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private const int DefaultBulkInsertBatchSize = 500;

    /// <summary>
    /// Inserts all rows from the supplied <see cref="DataTable"/> into the specified destination table.
    /// </summary>
    public virtual void BulkInsert(
        string database,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        SqliteTransaction? transaction = null;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);

            if (!useTransaction)
            {
                transaction = connection.BeginTransaction();
            }

            var totalRows = table.Rows.Count;
            if (totalRows == 0)
            {
                return;
            }

            var columns = GetColumns(table);
            var rowsPerBatch = ResolveRowsPerBatch(totalRows, batchSize);

            SqliteCommand? command = null;
            var preparedRowsPerBatch = 0;
            try
            {
                for (var offset = 0; offset < totalRows; offset += rowsPerBatch)
                {
                    var currentRows = Math.Min(rowsPerBatch, totalRows - offset);
                    if (command == null || preparedRowsPerBatch != currentRows)
                    {
                        command?.Dispose();
                        command = CreatePreparedBulkInsertCommand(connection, useTransaction ? _transaction : transaction, destinationTable, columns, currentRows, CommandTimeout);
                        preparedRowsPerBatch = currentRows;
                    }

                    ApplyBatchValues(command, columns, table, offset, currentRows);
                    ExecuteWithRetry(() => command.ExecuteNonQuery());
                }
            }
            finally
            {
                command?.Dispose();
            }

            if (!useTransaction)
            {
                transaction?.Commit();
            }
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (!useTransaction)
            {
                transaction?.Rollback();
            }

            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (!useTransaction)
            {
                transaction?.Dispose();
            }

            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously inserts all rows from the supplied <see cref="DataTable"/> into the specified destination table.
    /// </summary>
    public virtual async Task BulkInsertAsync(
        string database,
        DataTable table,
        string destinationTable,
        bool useTransaction = false,
        int? batchSize = null,
        CancellationToken cancellationToken = default)
    {
        if (table == null)
        {
            throw new ArgumentNullException(nameof(table));
        }

        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        SqliteTransaction? transaction = null;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);

            if (!useTransaction)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
                transaction = connection.BeginTransaction();
#endif
            }

            var totalRows = table.Rows.Count;
            if (totalRows == 0)
            {
                return;
            }

            var columns = GetColumns(table);
            var rowsPerBatch = ResolveRowsPerBatch(totalRows, batchSize);

            SqliteCommand? command = null;
            var preparedRowsPerBatch = 0;
            try
            {
                for (var offset = 0; offset < totalRows; offset += rowsPerBatch)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var currentRows = Math.Min(rowsPerBatch, totalRows - offset);
                    if (command == null || preparedRowsPerBatch != currentRows)
                    {
                        if (command != null)
                        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                            await command.DisposeAsync().ConfigureAwait(false);
#else
                            command.Dispose();
#endif
                        }

                        command = CreatePreparedBulkInsertCommand(connection, useTransaction ? _transaction : transaction, destinationTable, columns, currentRows, CommandTimeout);
                        preparedRowsPerBatch = currentRows;
                    }

                    ApplyBatchValues(command, columns, table, offset, currentRows);
                    await ExecuteWithRetryAsync(
                        async () => await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false),
                        cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                if (command != null)
                {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                    await command.DisposeAsync().ConfigureAwait(false);
#else
                    command.Dispose();
#endif
                }
            }

            if (!useTransaction)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                if (transaction != null)
                {
                    await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
#else
                transaction?.Commit();
#endif
            }
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex)
        {
            if (!useTransaction)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                if (transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
                }
#else
                transaction?.Rollback();
#endif
            }

            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (!useTransaction)
            {
                if (transaction != null)
                {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                    await transaction.DisposeAsync().ConfigureAwait(false);
#else
                    transaction.Dispose();
#endif
                }
            }

            if (dispose)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                if (connection != null)
                {
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
#else
                connection?.Dispose();
#endif
            }
        }
    }

    private static DataColumn[] GetColumns(DataTable table)
    {
        var columns = new DataColumn[table.Columns.Count];
        for (var i = 0; i < table.Columns.Count; i++)
        {
            columns[i] = table.Columns[i];
        }

        return columns;
    }

    private static int ResolveRowsPerBatch(int totalRows, int? batchSize)
    {
        if (totalRows <= 0)
        {
            return 1;
        }

        if (batchSize.HasValue && batchSize.Value > 0)
        {
            return batchSize.Value;
        }

        return Math.Min(totalRows, DefaultBulkInsertBatchSize);
    }

    private static string BuildBulkInsertStatement(string destinationTable, DataColumn[] columns, int rowsPerBatch)
    {
        var columnNames = new string[columns.Length];
        for (var i = 0; i < columns.Length; i++)
        {
            columnNames[i] = $"\"{columns[i].ColumnName}\"";
        }

        var values = new string[rowsPerBatch];
        for (var rowIndex = 0; rowIndex < rowsPerBatch; rowIndex++)
        {
            var parameterNames = new string[columns.Length];
            for (var colIndex = 0; colIndex < columns.Length; colIndex++)
            {
                parameterNames[colIndex] = GetParameterName(rowIndex, colIndex);
            }

            values[rowIndex] = $"({string.Join(", ", parameterNames)})";
        }

        return $"INSERT INTO {destinationTable} ({string.Join(", ", columnNames)}) VALUES {string.Join(", ", values)};";
    }

    private static SqliteCommand CreatePreparedBulkInsertCommand(SqliteConnection connection, SqliteTransaction? transaction, string destinationTable, DataColumn[] columns, int rowsPerBatch, int commandTimeout)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildBulkInsertStatement(destinationTable, columns, rowsPerBatch);
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        for (var rowIndex = 0; rowIndex < rowsPerBatch; rowIndex++)
        {
            for (var colIndex = 0; colIndex < columns.Length; colIndex++)
            {
                command.Parameters.AddWithValue(GetParameterName(rowIndex, colIndex), DBNull.Value);
            }
        }

        command.Prepare();
        return command;
    }

    private static string GetParameterName(int rowIndex, int colIndex) => $"@p{rowIndex}_{colIndex}";

    private static void ApplyBatchValues(SqliteCommand command, DataColumn[] columns, DataTable table, int offset, int rowsPerBatch)
    {
        var parameterIndex = 0;
        for (var rowIndex = 0; rowIndex < rowsPerBatch; rowIndex++)
        {
            var row = table.Rows[offset + rowIndex];
            for (var colIndex = 0; colIndex < columns.Length; colIndex++)
            {
                command.Parameters[parameterIndex].Value = row[columns[colIndex]];
                parameterIndex++;
            }
        }
    }
}
