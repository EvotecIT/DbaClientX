using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Executes a stored procedure using an existing collection of <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return ExecuteStoredProcedure(connectionString, procedure, parameters, useTransaction);
    }

    /// <summary>
    /// Executes a stored procedure using a full SQL Server connection string and existing <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual object? ExecuteStoredProcedure(
        string connectionString,
        string procedure,
        IEnumerable<DbParameter>? parameters,
        bool useTransaction = false)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, CommandType.StoredProcedure);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = transaction;
            AddParameters(command, parameters);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

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
        catch (SqlException ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        catch (InvalidOperationException ex)
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
    /// Asynchronously executes a stored procedure using an existing collection of <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return await ExecuteStoredProcedureAsync(connectionString, procedure, parameters, useTransaction, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes a stored procedure using a full SQL Server connection string and existing <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(
        string connectionString,
        string procedure,
        IEnumerable<DbParameter>? parameters,
        bool useTransaction = false,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(procedure, CommandType.StoredProcedure);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = transaction;
            AddParameters(command, parameters);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (SqlException ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        catch (InvalidOperationException ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
        }
    }
}
