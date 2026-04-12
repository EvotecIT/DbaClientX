using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Streams the results of a SQL query as an asynchronous sequence of <see cref="DataRow"/> instances.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildConnectionString(host, serviceName, username, password);
        return QueryStreamAsync(connectionString, query, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Streams the results of a SQL query from a full Oracle connection string as an asynchronous sequence of <see cref="DataRow"/> instances.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Streams the results of a SQL query through a caller-provided mapper.
    /// </summary>
    public virtual IAsyncEnumerable<T> QueryStreamAsync<T>(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        var connectionString = BuildConnectionString(host, serviceName, username, password);
        return QueryStreamAsync(connectionString, query, map, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections, initialize);
    }

    /// <summary>
    /// Streams the results of a SQL query from a full Oracle connection string through a caller-provided mapper.
    /// </summary>
    public virtual IAsyncEnumerable<T> QueryStreamAsync<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        return Stream();

        async IAsyncEnumerable<T> Stream()
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            var (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            try
            {
                await foreach (var row in ExecuteMappedQueryStreamAsync(connection, transaction, query, map, initialize, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Streams the results of an Oracle stored procedure execution as <see cref="DataRow"/> instances.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
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
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, serviceName, username, password);

            var (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, transaction, procedure, parameters, cancellationToken, dbTypes, parameterDirections, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Streams the results of an Oracle stored procedure execution as <see cref="DataRow"/> instances using explicitly defined parameters.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
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
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, serviceName, username, password);

            var (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, transaction, procedure, cancellationToken: cancellationToken, dbParameters: parameters, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                await DisposeOwnedResourceAsync(connection, dispose, DisposeConnectionAsync).ConfigureAwait(false);
            }
        }
    }
#endif
}
