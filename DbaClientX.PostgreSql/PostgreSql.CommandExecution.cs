using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>
    /// Executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    public virtual object? Query(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
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
    /// Executes a scalar SQL command and returns the first column of the first row in the result set.
    /// </summary>
    public virtual object? ExecuteScalar(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteScalar(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
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

    /// <summary>
    /// Executes a SQL command that does not produce a result set (such as <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    public virtual int ExecuteNonQuery(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
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

    private NpgsqlConnection ResolveConnection(string connectionString, bool useTransaction, out bool dispose)
    {
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            dispose = false;
            return _transactionConnection;
        }

        var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        dispose = true;
        return connection;
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, NpgsqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new NpgsqlParameter(), static (p, t) => p.NpgsqlDbType = t);
}
