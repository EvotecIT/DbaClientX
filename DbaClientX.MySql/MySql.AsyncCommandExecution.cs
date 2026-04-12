using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    /// <summary>
    /// Executes a SQL query asynchronously and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    public virtual async Task<object?> QueryAsync(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, database, username, password);
        return await QueryAsync(connectionString, query, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a SQL query asynchronously using a full MySQL connection string.
    /// </summary>
    public virtual async Task<object?> QueryAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteQueryAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes a SQL query asynchronously and maps each row with a caller-provided mapper.
    /// </summary>
    public virtual Task<IReadOnlyList<T>> QueryAsync<T>(
        string host,
        string database,
        string username,
        string password,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
        => QueryAsListAsync(host, database, username, password, query, map, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections);

    /// <summary>
    /// Executes a SQL query asynchronously and maps each row with a caller-provided mapper.
    /// </summary>
    public virtual Task<IReadOnlyList<T>> QueryAsListAsync<T>(
        string host,
        string database,
        string username,
        string password,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        var connectionString = BuildConnectionString(host, database, username, password);
        return QueryAsListAsync(connectionString, query, map, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections, initialize);
    }

    /// <summary>
    /// Executes a SQL query asynchronously using a full MySQL connection string and maps each row with a caller-provided mapper.
    /// </summary>
    public virtual Task<IReadOnlyList<T>> QueryAsync<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
        => QueryAsListAsync(connectionString, query, map, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections);

    /// <summary>
    /// Executes a SQL query asynchronously using a full MySQL connection string and maps each row with a caller-provided mapper.
    /// </summary>
    public virtual async Task<IReadOnlyList<T>> QueryAsListAsync<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteMappedQueryAsync(connection, transaction, query, map, initialize, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute mapped query.", query, ex);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes a SQL statement that does not produce a result set asynchronously.
    /// </summary>
    public virtual async Task<int> ExecuteNonQueryAsync(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, database, username, password);
        return await ExecuteNonQueryAsync(connectionString, query, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a SQL statement that does not produce a result set asynchronously using a full MySQL connection string.
    /// </summary>
    public virtual async Task<int> ExecuteNonQueryAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);
        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteNonQueryAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Executes a SQL query asynchronously and returns the first column of the first row in the result set.
    /// </summary>
    public virtual async Task<object?> ExecuteScalarAsync(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, database, username, password);
        return await ExecuteScalarAsync(connectionString, query, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a SQL query asynchronously using a full MySQL connection string and returns the first column of the first row in the result set.
    /// </summary>
    public virtual async Task<object?> ExecuteScalarAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);

        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteScalarAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    private async Task<(MySqlConnection Connection, MySqlTransaction? Transaction, bool Dispose)> ResolveConnectionAsync(string connectionString, bool useTransaction, CancellationToken cancellationToken)
    {
        if (useTransaction)
        {
            lock (_syncRoot)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }

                var normalizedConnectionString = NormalizeConnectionString(connectionString);
                if (_transactionConnectionString != null && !string.Equals(_transactionConnectionString, normalizedConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    throw new DbaTransactionException("The requested connection details do not match the active transaction.");
                }

                return (_transactionConnection, _transaction, false);
            }
        }

        var connection = CreateConnection(connectionString);
        try
        {
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            return (connection, null, true);
        }
        catch
        {
            await DisposeOwnedResourceAsync(connection, ownsResource: true, DisposeConnectionAsync).ConfigureAwait(false);
            throw;
        }
    }
}
