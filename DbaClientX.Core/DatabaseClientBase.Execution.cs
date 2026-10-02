using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    /// <summary>
    /// Executes a query and materializes the result according to <see cref="ReturnType"/>.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The query result shaped according to <see cref="ReturnType"/>.</returns>
    protected virtual object? ExecuteQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        return ExecuteCommandWithRetry<object?>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            object? result;
            using (var reader = command.ExecuteReader(CommandBehavior.SequentialAccess))
            {
                var returnType = ReturnType;
                if (returnType == ReturnType.DataRow)
                {
                    if (reader.Read())
                    {
                        var table = new DataTable("Table0");
                        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            table.Columns.Add(GetUniqueColumnName(reader.GetName(i), i, columnNames), reader.GetFieldType(i));
                        }
                        var row = table.NewRow();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                        }
                        result = row;
                    }
                    else
                    {
                        result = null;
                    }
                }
                else if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
                {
                    result = ReadDataTable(reader, "Table0", AdaptResultColumnTypesToValues);
                }
                else
                {
                    var dataSet = new DataSet();
                    var tableIndex = 0;
                    do
                    {
                        var table = ReadDataTable(reader, $"Table{tableIndex}", AdaptResultColumnTypesToValues);
                        dataSet.Tables.Add(table);
                        tableIndex++;
                    } while (!reader.IsClosed && reader.NextResult());

                    result = BuildResult(dataSet);
                }
            }

            UpdateOutputParameters(command, parameters);
            return result;
        }, transaction);
    }

    /// <summary>
    /// Executes a query and maps each row with a caller-provided mapper.
    /// </summary>
    /// <typeparam name="T">The row result type produced by <paramref name="map"/>.</typeparam>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="map">A mapper that converts the current data record into a result value.</param>
    /// <param name="initialize">Optional callback invoked once after the reader opens and before the first row is read.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The mapped result rows.</returns>
    protected virtual IReadOnlyList<T> ExecuteMappedQuery<T>(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        Func<IDataRecord, T> map,
        Action<IDataRecord>? initialize = null,
        IDictionary<string, object?>? parameters = null,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        if (map == null)
        {
            throw new ArgumentNullException(nameof(map));
        }

        return ExecuteCommandWithRetry<IReadOnlyList<T>>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var rows = new List<T>();
            using (var reader = command.ExecuteReader(CommandBehavior.Default))
            {
                initialize?.Invoke(reader);
                while (reader.Read())
                {
                    rows.Add(map(reader));
                }
            }

            UpdateOutputParameters(command, parameters);
            return rows;
        }, transaction);
    }

    /// <summary>
    /// Executes a non-query command (INSERT/UPDATE/DELETE) with retry support.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The number of rows affected.</returns>
    protected virtual int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        int ExecuteOperation()
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var affected = command.ExecuteNonQuery();
            UpdateOutputParameters(command, parameters);
            return affected;
        }

        return ExecuteCommandWithRetry(ExecuteOperation, transaction, returnsResults: false);
    }

    /// <summary>
    /// Executes a scalar command and returns the first column of the first row.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The scalar result.</returns>
    protected virtual object? ExecuteScalar(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        return ExecuteCommandWithRetry<object?>(() =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);
            var result = command.ExecuteScalar();
            UpdateOutputParameters(command, parameters);
            return result;
        }, transaction);
    }

    /// <summary>
    /// Asynchronously executes a query and materializes the result according to <see cref="ReturnType"/>.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The query result shaped according to <see cref="ReturnType"/>.</returns>
    protected virtual async Task<object?> ExecuteQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        return await ExecuteCommandWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            object? result;
            using (var reader = await AwaitWithCallerCancellationAsync(
                () => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken),
                cancellationToken).ConfigureAwait(false))
            {
                var returnType = ReturnType;
                if (returnType == ReturnType.DataRow)
                {
                    if (await AwaitWithCallerCancellationAsync(
                        () => reader.ReadAsync(cancellationToken),
                        cancellationToken).ConfigureAwait(false))
                    {
                        var table = new DataTable("Table0");
                        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            table.Columns.Add(GetUniqueColumnName(reader.GetName(i), i, columnNames), reader.GetFieldType(i));
                        }
                        var row = table.NewRow();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            row[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
                        }
                        result = row;
                    }
                    else
                    {
                        result = null;
                    }
                }
                else if (returnType == ReturnType.DataTable || returnType == ReturnType.PSObject)
                {
                    result = await ReadDataTableAsync(reader, "Table0", AdaptResultColumnTypesToValues, cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var dataSet = new DataSet();
                    var tableIndex = 0;
                    do
                    {
                        var table = await ReadDataTableAsync(reader, $"Table{tableIndex}", AdaptResultColumnTypesToValues, cancellationToken).ConfigureAwait(false);
                        dataSet.Tables.Add(table);
                        tableIndex++;
                    } while (!reader.IsClosed && await AwaitWithCallerCancellationAsync(
                        () => reader.NextResultAsync(cancellationToken),
                        cancellationToken).ConfigureAwait(false));

                    result = BuildResult(dataSet);
                }
            }

            UpdateOutputParameters(command, parameters);
            return result;
        }, transaction, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes a query and maps each row with a caller-provided mapper.
    /// </summary>
    /// <typeparam name="T">The row result type produced by <paramref name="map"/>.</typeparam>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="map">A mapper that converts the current data record into a result value.</param>
    /// <param name="initialize">Optional callback invoked once after the reader opens and before the first row is read.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The mapped result rows.</returns>
    protected virtual async Task<IReadOnlyList<T>> ExecuteMappedQueryAsync<T>(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        Func<IDataRecord, T> map,
        Action<IDataRecord>? initialize = null,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        if (map == null)
        {
            throw new ArgumentNullException(nameof(map));
        }

        return await ExecuteCommandWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var rows = new List<T>();
            using (var reader = await AwaitWithCallerCancellationAsync(
                () => command.ExecuteReaderAsync(CommandBehavior.Default, cancellationToken),
                cancellationToken).ConfigureAwait(false))
            {
                initialize?.Invoke(reader);
                while (await AwaitWithCallerCancellationAsync(
                    () => reader.ReadAsync(cancellationToken),
                    cancellationToken).ConfigureAwait(false))
                {
                    rows.Add(map(reader));
                }
            }

            UpdateOutputParameters(command, parameters);
            return (IReadOnlyList<T>)rows;
        }, transaction, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes a non-query command (INSERT/UPDATE/DELETE) with retry support.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The number of rows affected.</returns>
    protected virtual async Task<int> ExecuteNonQueryAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        async Task<int> ExecuteOperationAsync()
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var affected = await AwaitWithCallerCancellationAsync(
                () => command.ExecuteNonQueryAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);
            UpdateOutputParameters(command, parameters);
            return affected;
        }

        return await ExecuteCommandWithRetryAsync(
            ExecuteOperationAsync, transaction, cancellationToken, returnsResults: false).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes a scalar command and returns the first column of the first row.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The command text to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <returns>The scalar result.</returns>
    protected virtual async Task<object?> ExecuteScalarAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        return await ExecuteCommandWithRetryAsync(async () =>
        {
            using var command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var result = await AwaitWithCallerCancellationAsync(
                () => command.ExecuteScalarAsync(cancellationToken),
                cancellationToken).ConfigureAwait(false);
            UpdateOutputParameters(command, parameters);
            return result;
        }, transaction, cancellationToken).ConfigureAwait(false);
    }
}
