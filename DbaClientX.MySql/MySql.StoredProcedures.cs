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
    /// Executes a stored procedure and returns the aggregated results.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string host,
        string database,
        string username,
        string password,
        string procedure,
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

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = command.ExecuteReader();
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            }
            while (!reader.IsClosed && reader.NextResult());

            var result = BuildResult(dataSet);
            UpdateOutputParameters(command, parameters);
            return result;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
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
    /// Executes a stored procedure asynchronously and returns the aggregated results.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string host,
        string database,
        string username,
        string password,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            }
            while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

            var result = BuildResult(dataSet);
            UpdateOutputParameters(command, parameters);
            return result;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
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
    /// Executes a stored procedure using explicitly configured <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string host,
        string database,
        string username,
        string password,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
        var dispose = false;
        try
        {
            connection = ResolveConnection(connectionString, useTransaction, out dispose);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            AddParameters(command, parameters);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = command.ExecuteReader();
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            }
            while (!reader.IsClosed && reader.NextResult());

            return BuildResult(dataSet);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
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
    /// Executes a stored procedure asynchronously using explicitly configured <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string host,
        string database,
        string username,
        string password,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            AddParameters(command, parameters);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            }
            while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

            return BuildResult(dataSet);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    private void ApplyCommandTimeout(MySqlCommand command)
    {
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }
    }
}
