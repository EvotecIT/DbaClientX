using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
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

        var connectionString = BuildConnectionString(database);

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
            var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
            var rowsPerBatch = batchSize.HasValue && batchSize.Value > 0 ? batchSize.Value : totalRows;

            for (var offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                var sb = new StringBuilder();
                sb.Append($"INSERT INTO {destinationTable} ({columns}) VALUES ");

                var parameters = new Dictionary<string, object?>();
                var paramIndex = 0;
                var max = Math.Min(offset + rowsPerBatch, totalRows);
                for (var i = offset; i < max; i++)
                {
                    if (i > offset)
                    {
                        sb.Append(", ");
                    }

                    sb.Append("(");
                    var colIndex = 0;
                    foreach (DataColumn column in table.Columns)
                    {
                        var paramName = $"@p{paramIndex++}";
                        parameters[paramName] = table.Rows[i][column] ?? DBNull.Value;
                        if (colIndex > 0)
                        {
                            sb.Append(", ");
                        }

                        sb.Append(paramName);
                        colIndex++;
                    }

                    sb.Append(")");
                }

                sb.Append(";");

                ExecuteNonQuery(connection, useTransaction ? _transaction : transaction, sb.ToString(), parameters);
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

        var connectionString = BuildConnectionString(database);

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
            var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
            var rowsPerBatch = batchSize.HasValue && batchSize.Value > 0 ? batchSize.Value : totalRows;

            for (var offset = 0; offset < totalRows; offset += rowsPerBatch)
            {
                var sb = new StringBuilder();
                sb.Append($"INSERT INTO {destinationTable} ({columns}) VALUES ");

                var parameters = new Dictionary<string, object?>();
                var paramIndex = 0;
                var max = Math.Min(offset + rowsPerBatch, totalRows);
                for (var i = offset; i < max; i++)
                {
                    if (i > offset)
                    {
                        sb.Append(", ");
                    }

                    sb.Append("(");
                    var colIndex = 0;
                    foreach (DataColumn column in table.Columns)
                    {
                        var paramName = $"@p{paramIndex++}";
                        parameters[paramName] = table.Rows[i][column] ?? DBNull.Value;
                        if (colIndex > 0)
                        {
                            sb.Append(", ");
                        }

                        sb.Append(paramName);
                        colIndex++;
                    }

                    sb.Append(")");
                }

                sb.Append(";");

                await ExecuteNonQueryAsync(connection, useTransaction ? _transaction : transaction, sb.ToString(), parameters, cancellationToken).ConfigureAwait(false);
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
}
