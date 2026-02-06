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
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return base.ExecuteQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
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
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return base.ExecuteScalar(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
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
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return base.ExecuteNonQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
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

    private SqliteConnection ResolveConnection(string connectionString, bool useTransaction, out bool dispose, int? busyTimeoutMs = null)
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

        var connection = new SqliteConnection(connectionString);
        connection.Open();
        ApplyBusyTimeout(connection, busyTimeoutMs);
        dispose = true;
        return connection;
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqliteType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new SqliteParameter(), static (p, t) => p.SqliteType = t);
}
