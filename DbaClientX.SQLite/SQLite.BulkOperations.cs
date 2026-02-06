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

            using var command = CreatePreparedBulkInsertCommand(connection, useTransaction ? _transaction : transaction, destinationTable, columns, CommandTimeout);

            for (var offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                var max = Math.Min(offset + rowsPerBatch, totalRows);
                for (var rowIndex = offset; rowIndex < max; rowIndex++)
                {
                    ApplyRowValues(command, columns, table.Rows[rowIndex]);
                    ExecuteWithRetry(() => command.ExecuteNonQuery());
                }
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

            using var command = CreatePreparedBulkInsertCommand(connection, useTransaction ? _transaction : transaction, destinationTable, columns, CommandTimeout);

            for (var offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var max = Math.Min(offset + rowsPerBatch, totalRows);
                for (var rowIndex = offset; rowIndex < max; rowIndex++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    ApplyRowValues(command, columns, table.Rows[rowIndex]);
                    await ExecuteWithRetryAsync(
                        async () => await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false),
                        cancellationToken).ConfigureAwait(false);
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

    private static string BuildBulkInsertStatement(string destinationTable, DataColumn[] columns)
    {
        var columnNames = new string[columns.Length];
        var parameterNames = new string[columns.Length];
        for (var i = 0; i < columns.Length; i++)
        {
            columnNames[i] = $"\"{columns[i].ColumnName}\"";
            parameterNames[i] = $"@p{i}";
        }

        return $"INSERT INTO {destinationTable} ({string.Join(", ", columnNames)}) VALUES ({string.Join(", ", parameterNames)});";
    }

    private static SqliteCommand CreatePreparedBulkInsertCommand(SqliteConnection connection, SqliteTransaction? transaction, string destinationTable, DataColumn[] columns, int commandTimeout)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = BuildBulkInsertStatement(destinationTable, columns);
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        for (var i = 0; i < columns.Length; i++)
        {
            command.Parameters.AddWithValue($"@p{i}", DBNull.Value);
        }

        command.Prepare();
        return command;
    }

    private static void ApplyRowValues(SqliteCommand command, DataColumn[] columns, DataRow row)
    {
        for (var i = 0; i < columns.Length; i++)
        {
            var value = row[columns[i]];
            command.Parameters[i].Value = value == DBNull.Value ? DBNull.Value : value;
        }
    }
}
