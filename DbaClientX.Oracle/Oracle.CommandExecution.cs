using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    /// <summary>
    /// Executes a SQL query and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    public virtual object? Query(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
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
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
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
    /// Executes a SQL statement that does not return rows, such as INSERT, UPDATE, or DELETE.
    /// </summary>
    public virtual int ExecuteNonQuery(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
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

    protected virtual OracleConnection ResolveConnection(string connectionString, bool useTransaction, out bool dispose)
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

        var connection = CreateConnection(connectionString);
        connection.Open();
        EnlistInDistributedTransaction(connection);
        dispose = true;
        return connection;
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, OracleDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new OracleParameter(), static (p, t) => p.OracleDbType = t);

    /// <summary>
    /// Enlists a connection in an ambient distributed transaction when available.
    /// </summary>
    /// <param name="connection">Connection to enlist.</param>
    /// <returns><see langword="true"/> when enlistment succeeds.</returns>
    protected virtual bool EnlistInDistributedTransaction(OracleConnection connection) => TryEnlistInAmbientTransaction(connection);

    /// <summary>
    /// Asynchronously enlists a connection in an ambient distributed transaction when available.
    /// </summary>
    /// <param name="connection">Connection to enlist.</param>
    /// <param name="cancellationToken">Token used to cancel the enlistment attempt.</param>
    /// <returns><see langword="true"/> when enlistment succeeds.</returns>
    protected virtual Task<bool> EnlistInDistributedTransactionAsync(OracleConnection connection, CancellationToken cancellationToken) =>
        TryEnlistInAmbientTransactionAsync(connection, cancellationToken);
}
