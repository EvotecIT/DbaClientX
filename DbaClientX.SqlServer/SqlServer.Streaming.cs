#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Asynchronously executes a SQL query and streams the resulting rows.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(
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
        return QueryStreamAsync(connectionString, query, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections);
    }

    /// <summary>
    /// Asynchronously executes a SQL query using a full SQL Server connection string and streams the resulting rows.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(
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
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            SqlConnection? connection = null;
            SqlTransaction? transaction = null;
            var dispose = false;
            try
            {
                (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
                var dbTypes = ConvertParameterTypes(parameterTypes);
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
    /// Asynchronously executes a SQL query and streams rows through a caller-provided mapper.
    /// </summary>
    public virtual IAsyncEnumerable<T> QueryStreamAsync<T>(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
        return QueryStreamAsync(connectionString, query, map, parameters, useTransaction, cancellationToken, parameterTypes, parameterDirections, initialize);
    }

    /// <summary>
    /// Asynchronously executes a SQL query using a full SQL Server connection string and streams rows through a caller-provided mapper.
    /// </summary>
    public virtual IAsyncEnumerable<T> QueryStreamAsync<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqlDbType>? parameterTypes = null,
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
    /// Asynchronously executes a stored procedure and streams the resulting rows.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        ValidateCommandText(procedure, CommandType.StoredProcedure);
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            SqlConnection? connection = null;
            SqlTransaction? transaction = null;
            var dispose = false;
            try
            {
                (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
                var dbTypes = ConvertParameterTypes(parameterTypes);
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
    /// Asynchronously executes a stored procedure using <see cref="DbParameter"/> instances and streams the resulting rows.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
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
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            SqlConnection? connection = null;
            SqlTransaction? transaction = null;
            var dispose = false;
            try
            {
                (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
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
}
#endif
