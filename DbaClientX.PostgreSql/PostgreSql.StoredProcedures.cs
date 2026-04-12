using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>
    /// Executes a stored procedure and materializes the results into the shared <see cref="DatabaseClientBase"/> format.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string host,
        string database,
        string username,
        string password,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, database, username, password);
        return ExecuteStoredProcedure(connectionString, procedure, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Executes a stored procedure using a full PostgreSQL connection string and materializes the results into the shared format.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string connectionString,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, CommandType.StoredProcedure);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = transaction;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
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
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a stored procedure using a full PostgreSQL connection string and materializes the results into the shared format.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string connectionString,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, CommandType.StoredProcedure);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = transaction;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
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
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Asynchronously executes a stored procedure and materializes the results into the shared <see cref="DatabaseClientBase"/> format.
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
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, database, username, password);
        return await ExecuteStoredProcedureAsync(connectionString, procedure, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a stored procedure using pre-constructed database parameters.
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
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = transaction;
            AddParameters(command, parameters);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
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
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a stored procedure using pre-constructed database parameters.
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
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = transaction;
            AddParameters(command, parameters);
            ApplyCommandTimeout(command);

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
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
                DisposeConnection(connection!);
            }
        }
    }

    private void ApplyCommandTimeout(NpgsqlCommand command)
    {
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }
    }
}
