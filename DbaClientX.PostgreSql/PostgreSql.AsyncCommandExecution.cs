using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>
    /// Asynchronously executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
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
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
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
    /// Asynchronously executes a SQL command that does not produce a result set (such as <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
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
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteNonQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
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
    /// Asynchronously executes a scalar SQL command and returns the first column of the first row in the result set.
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
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteScalarAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    private async Task<(NpgsqlConnection Connection, bool Dispose)> ResolveConnectionAsync(string connectionString, bool useTransaction, CancellationToken cancellationToken)
    {
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            return (_transactionConnection, false);
        }

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return (connection, true);
    }
}
