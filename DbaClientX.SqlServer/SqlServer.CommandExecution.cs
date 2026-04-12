using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    public virtual object? Query(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return Query(connectionString, query, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Executes a SQL query using a full SQL Server connection string and materializes the result using the shared pipeline.
    /// </summary>
    public virtual object? Query(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
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
    /// Executes a SQL query that returns a single scalar value.
    /// </summary>
    public virtual object? ExecuteScalar(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return ExecuteScalar(connectionString, query, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Executes a SQL query using a full SQL Server connection string and returns a single scalar value.
    /// </summary>
    public virtual object? ExecuteScalar(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteScalar(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
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
    /// Executes a SQL statement that does not return a result set (for example, <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    public virtual int ExecuteNonQuery(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return ExecuteNonQuery(connectionString, query, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Executes a SQL statement using a full SQL Server connection string.
    /// </summary>
    public virtual int ExecuteNonQuery(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }
}
