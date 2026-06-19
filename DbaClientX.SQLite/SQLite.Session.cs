using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Opens a managed SQLite session that reuses one connection for a sequence of commands.
    /// </summary>
    /// <param name="database">Absolute or relative path of the SQLite database file.</param>
    /// <returns>A session that owns the underlying provider connection.</returns>
    /// <remarks>
    /// Use this for workflows that require connection-local state such as attached databases,
    /// temporary tables, or explicit transaction blocks without exposing provider objects.
    /// </remarks>
    public virtual SQLiteSession OpenSession(string database)
    {
        var connectionString = BuildOperationalConnectionString(database);
        SqliteConnection? connection = null;
        try
        {
            (connection, _, _) = ResolveConnection(connectionString, useTransaction: false);
            return new SQLiteSession(this, connection);
        }
        catch
        {
            connection?.Dispose();
            throw;
        }
    }

    internal int ExecuteSessionNonQuery(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters)
    {
        ValidateCommandText(query);
        try
        {
            return ExecuteNonQuery(connection, transaction, query, parameters);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
    }

    internal object? ExecuteSessionScalar(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters)
    {
        ValidateCommandText(query);
        try
        {
            return ExecuteScalar(connection, transaction, query, parameters);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
    }

    internal IReadOnlyList<T> ExecuteSessionQueryAsList<T>(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters,
        Action<IDataRecord>? initialize)
    {
        ValidateCommandText(query);
        if (map == null)
        {
            throw new ArgumentNullException(nameof(map));
        }

        try
        {
            return ExecuteWithRetry(() =>
            {
                using var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = query;
                AddParameters(command, parameters);
                var commandTimeout = CommandTimeout;
                if (commandTimeout > 0)
                {
                    command.CommandTimeout = commandTimeout;
                }

                using var reader = command.ExecuteReader(CommandBehavior.Default);
                initialize?.Invoke(reader);

                List<T> results = new();
                while (reader.Read())
                {
                    results.Add(map(reader));
                }

                UpdateOutputParameters(command, parameters);
                return (IReadOnlyList<T>)results;
            });
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute mapped query.", query, ex);
        }
    }

    internal TResult ExecuteSessionTransaction<TResult>(
        SqliteConnection connection,
        Func<SQLiteSession, TResult> operation)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        using SqliteTransaction transaction = connection.BeginTransaction();
        SQLiteSession transactionSession = new(this, connection, transaction);
        try
        {
            TResult result = operation(transactionSession);
            transaction.Commit();
            return result;
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }
}
