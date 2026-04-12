using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    /// <summary>
    /// Executes an Oracle stored procedure and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string host,
        string serviceName,
        string username,
        string password,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        return ExecuteStoredProcedure(connectionString, procedure, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Executes an Oracle stored procedure using a full connection string and materializes the results into the default return format.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string connectionString,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, CommandType.StoredProcedure);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
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
                var table = ReadDataTable(reader, $"Table{tableIndex}");
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
    /// Asynchronously executes an Oracle stored procedure using a full connection string and materializes the results into the default return format.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string connectionString,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, CommandType.StoredProcedure);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
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
                var table = await ReadDataTableAsync(reader, $"Table{tableIndex}", cancellationToken).ConfigureAwait(false);
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
    /// Asynchronously executes an Oracle stored procedure and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string host,
        string serviceName,
        string username,
        string password,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        return await ExecuteStoredProcedureAsync(connectionString, procedure, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes an Oracle stored procedure using explicitly configured <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string host,
        string serviceName,
        string username,
        string password,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
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
                var table = ReadDataTable(reader, $"Table{tableIndex}");
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
    /// Asynchronously executes an Oracle stored procedure using explicitly configured <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string host,
        string serviceName,
        string username,
        string password,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
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
                var table = await ReadDataTableAsync(reader, $"Table{tableIndex}", cancellationToken).ConfigureAwait(false);
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

    private void ApplyCommandTimeout(OracleCommand command)
    {
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }
    }
}
