using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
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
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
    /// Executes a SQL query and returns the first column of the first row in the result set.
    /// </summary>
    public virtual object? ExecuteScalar(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
    /// Executes a SQL statement that does not produce a result set.
    /// </summary>
    public virtual int ExecuteNonQuery(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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

    private MySqlConnection ResolveConnection(string connectionString, bool useTransaction, out bool dispose)
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

        var connection = new MySqlConnection(connectionString);
        connection.Open();
        dispose = true;
        return connection;
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, MySqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new MySqlParameter(), static (p, t) => p.MySqlDbType = t);
}
