using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Asynchronously executes a SQL query and materializes the result using the shared pipeline from <see cref="DatabaseClientBase"/>.
    /// </summary>
    public virtual async Task<object?> QueryAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose && connection != null)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL query against a read-only connection and materializes the result using the shared pipeline from <see cref="DatabaseClientBase"/>.
    /// </summary>
    public virtual async Task<object?> QueryReadOnlyAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        int? busyTimeoutMs = null,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildOperationalConnectionString(database, readOnly: true);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction: false, cancellationToken, busyTimeoutMs).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteQueryAsync(connection, null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose && connection != null)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL query against a read-only connection and maps each returned row using the provided callback.
    /// </summary>
    /// <typeparam name="T">The type produced by the row mapping callback.</typeparam>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="map">Callback used to map the current <see cref="DbDataReader"/> row to <typeparamref name="T"/>.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel command execution.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout in milliseconds.</param>
    /// <param name="parameterTypes">Optional provider-specific parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <param name="initialize">
    /// Optional callback invoked once after the reader is opened and before the first row is read. Use it to cache ordinals.
    /// </param>
    /// <returns>A read-only list of mapped rows.</returns>
    public virtual async Task<IReadOnlyList<T>> QueryReadOnlyAsListAsync<T>(
        string database,
        string query,
        Func<DbDataReader, T> map,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        int? busyTimeoutMs = null,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<DbDataReader>? initialize = null)
    {
        if (string.IsNullOrWhiteSpace(query)) throw new ArgumentNullException(nameof(query));
        if (map == null) throw new ArgumentNullException(nameof(map));

        var connectionString = BuildOperationalConnectionString(database, readOnly: true);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction: false, cancellationToken, busyTimeoutMs).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);

            var list = await ExecuteWithRetryAsync(async () =>
            {
                using var command = connection.CreateCommand();
                command.CommandText = query;
                AddParameters(command, parameters, dbTypes, parameterDirections);
                var commandTimeout = CommandTimeout;
                if (commandTimeout > 0)
                {
                    command.CommandTimeout = commandTimeout;
                }

                using var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult, cancellationToken).ConfigureAwait(false);
                initialize?.Invoke(reader);

                var results = new List<T>();
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    results.Add(map(reader));
                }

                UpdateOutputParameters(command, parameters);
                return (IReadOnlyList<T>)results;
            }, cancellationToken).ConfigureAwait(false);

            return list;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute mapped query.", query, ex);
        }
        finally
        {
            if (dispose && connection != null)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL statement that does not return rows.
    /// </summary>
    public virtual async Task<int> ExecuteNonQueryAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteNonQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose && connection != null)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL query that returns a single scalar value.
    /// </summary>
    public virtual async Task<object?> ExecuteScalarAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;
        try
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteScalarAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose && connection != null)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }

    private async Task<(SqliteConnection Connection, bool Dispose)> ResolveConnectionAsync(string connectionString, bool useTransaction, CancellationToken cancellationToken, int? busyTimeoutMs = null)
    {
        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            return (_transactionConnection, false);
        }

        var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        await ApplyBusyTimeoutAsync(connection, busyTimeoutMs, cancellationToken).ConfigureAwait(false);
        return (connection, true);
    }
}
