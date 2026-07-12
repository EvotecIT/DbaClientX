using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    public virtual object? Query(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return base.ExecuteQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
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
    /// Executes a SQL query that returns a single scalar value.
    /// </summary>
    public virtual object? ExecuteScalar(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return base.ExecuteScalar(connection, transaction, query, parameters, dbTypes, parameterDirections);
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
    /// Executes a SQL query that returns a single scalar value using a SQLite connection string.
    /// </summary>
    public virtual object? ExecuteScalarWithConnectionString(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var normalizedConnectionString = NormalizeConnectionString(connectionString);

        try
        {
            var (connection, transaction, dispose) = ResolveConnection(normalizedConnectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            if (dispose)
            {
                using (connection)
                {
                    return base.ExecuteScalar(connection, transaction, query, parameters, dbTypes, parameterDirections);
                }
            }

            return base.ExecuteScalar(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (DbaTransactionException)
        {
            throw;
        }
        catch (Exception ex) when (ex is DbException or InvalidOperationException or ArgumentException)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
    }

    /// <summary>
    /// Executes a SQL statement that does not return rows (for example <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    public virtual int ExecuteNonQuery(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildOperationalConnectionString(database);
        return ExecuteNonQueryCore(connectionString, query, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Executes a SQL statement that does not return rows using a full SQLite connection string.
    /// </summary>
    public virtual int ExecuteNonQueryWithConnectionString(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var normalizedConnectionString = NormalizeConnectionString(connectionString);
        return ExecuteNonQueryCore(normalizedConnectionString, query, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    private int ExecuteNonQueryCore(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters,
        bool useTransaction,
        IDictionary<string, SqliteType>? parameterTypes,
        IDictionary<string, ParameterDirection>? parameterDirections)
    {
        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return base.ExecuteNonQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
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

    private (SqliteConnection Connection, SqliteTransaction? Transaction, bool Dispose) ResolveConnection(string connectionString, bool useTransaction, int? busyTimeoutMs = null)
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

        var connection = new SqliteConnection(connectionString);
        try
        {
            connection.Open();
            ApplyBusyTimeout(connection, ResolveConnectionBusyTimeout(connectionString, busyTimeoutMs));
            return (connection, null, true);
        }
        catch
        {
            connection.Dispose();
            throw;
        }
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqliteType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t);
}
