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
    /// Opens a SQL Server query as a streaming <see cref="IDataReader"/>.
    /// </summary>
    /// <remarks>
    /// The returned reader owns the command and, when DbaClientX opened it, the connection. Dispose the reader after
    /// the consuming API has finished reading.
    /// </remarks>
    public virtual IDataReader QueryReader(
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
        return QueryReader(connectionString, query, parameters, useTransaction, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Opens a SQL Server query as a streaming <see cref="IDataReader"/> using a full connection string.
    /// </summary>
    /// <remarks>
    /// The returned reader owns the command and, when DbaClientX opened it, the connection. Dispose the reader after
    /// the consuming API has finished reading.
    /// </remarks>
    public virtual IDataReader QueryReader(
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
        SqlCommand? command = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            command = CreateQueryReaderCommand(connection, transaction, query, parameters, parameterTypes, parameterDirections);
            var reader = ExecuteReader(command, CancellationToken.None);
            return new DbaDataReader(
                reader,
                command,
                connection,
                dispose,
                resource => DisposeConnection((SqlConnection)resource),
                () => UpdateOutputParameters(command, parameters));
        }
        catch (Exception ex)
        {
            command?.Dispose();
            if (connection != null && dispose)
            {
                DisposeConnection(connection);
            }

            throw new DbaQueryExecutionException("Failed to open query reader.", query, ex);
        }
    }

    /// <summary>
    /// Opens a SQL Server query as a streaming <see cref="IDataReader"/> asynchronously.
    /// </summary>
    /// <remarks>
    /// The returned reader owns the command and, when DbaClientX opened it, the connection. Dispose the reader after
    /// the consuming API has finished reading.
    /// </remarks>
    public virtual async Task<IDataReader> QueryReaderAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return await QueryReaderAsync(connectionString, query, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a SQL Server query as a streaming <see cref="IDataReader"/> asynchronously using a full connection string.
    /// </summary>
    /// <remarks>
    /// The returned reader owns the command and, when DbaClientX opened it, the connection. Dispose the reader after
    /// the consuming API has finished reading.
    /// </remarks>
    public virtual async Task<IDataReader> QueryReaderAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);

        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        SqlCommand? command = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            command = CreateQueryReaderCommand(connection, transaction, query, parameters, parameterTypes, parameterDirections);
            var reader = await ExecuteReaderAsync(command, cancellationToken).ConfigureAwait(false);
            return new DbaDataReader(
                reader,
                command,
                connection,
                dispose,
                resource => DisposeConnection((SqlConnection)resource),
                () => UpdateOutputParameters(command, parameters));
        }
        catch (Exception ex)
        {
            command?.Dispose();
            if (connection != null && dispose)
            {
                await DisposeOwnedResourceAsync(connection, ownsResource: true, DisposeConnectionAsync).ConfigureAwait(false);
            }

            if (IsCallerCancellation(ex, cancellationToken))
            {
                throw;
            }

            throw CreateQueryExecutionOrCancellationException("Failed to open query reader.", query, ex, cancellationToken);
        }
    }

    private SqlCommand CreateQueryReaderCommand(
        SqlConnection connection,
        SqlTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters,
        IDictionary<string, SqlDbType>? parameterTypes,
        IDictionary<string, ParameterDirection>? parameterDirections)
    {
        var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        var dbTypes = ConvertParameterTypes(parameterTypes);
        AddParameters(command, parameters, dbTypes, parameterDirections);
        var commandTimeout = CommandTimeout;
        if (commandTimeout > 0)
        {
            command.CommandTimeout = commandTimeout;
        }

        return command;
    }

    private DbDataReader ExecuteReader(DbCommand command, CancellationToken cancellationToken)
    {
        var maxAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts;
        var attempt = 0;
        while (true)
        {
            try
            {
                return command.ExecuteReader(CommandBehavior.SequentialAccess);
            }
            catch (Exception ex) when (IsTransient(ex) && ++attempt < maxAttempts)
            {
                var delay = ComputeBackoffDelay(attempt);
                if (delay > TimeSpan.Zero)
                {
                    cancellationToken.WaitHandle.WaitOne(delay);
                }
            }
        }
    }

    private async Task<DbDataReader> ExecuteReaderAsync(DbCommand command, CancellationToken cancellationToken)
    {
        var maxAttempts = MaxRetryAttempts < 1 ? 1 : MaxRetryAttempts;
        var attempt = 0;
        while (true)
        {
            try
            {
                return await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransient(ex) && ++attempt < maxAttempts)
            {
                var delay = ComputeBackoffDelay(attempt);
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }
}
