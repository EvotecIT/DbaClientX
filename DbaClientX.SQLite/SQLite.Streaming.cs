#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Streams query results asynchronously, yielding one <see cref="DataRow"/> at a time.
    /// </summary>
    /// <remarks>
    /// SQLite columns can mix storage classes across rows. When a later row needs a different column type (for example a
    /// TEXT value after INTEGER values), rows from that point are created from a new <see cref="DataTable"/> with the adapted
    /// schema, so do not assume every streamed row shares the schema of the first row's <see cref="DataRow.Table"/>.
    /// </remarks>
    public virtual async IAsyncEnumerable<DataRow> QueryStreamAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        var dispose = false;

        if (useTransaction)
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, true, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, false, cancellationToken).ConfigureAwait(false);
        }

        var dbTypes = ConvertParameterTypes(parameterTypes);
        if (connection == null)
        {
            throw CreateQueryExecutionException(
                "Failed to resolve connection for streaming.",
                query,
                new InvalidOperationException("The SQLite connection could not be resolved."));
        }

        try
        {
            await foreach (var row in base.ExecuteQueryStreamAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
            {
                yield return row;
            }
        }
        finally
        {
            if (dispose)
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
    /// Streams query results asynchronously from a SQLite connection string, yielding one <see cref="DataRow"/> at a time.
    /// </summary>
    /// <remarks>
    /// SQLite columns can mix storage classes across rows. When a later row needs a different column type (for example a
    /// TEXT value after INTEGER values), rows from that point are created from a new <see cref="DataTable"/> with the adapted
    /// schema, so do not assume every streamed row shares the schema of the first row's <see cref="DataRow.Table"/>.
    /// </remarks>
    public virtual async IAsyncEnumerable<DataRow> QueryStreamWithConnectionStringAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateCommandText(query);
        var normalizedConnectionString = NormalizeConnectionString(connectionString);

        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        var dispose = false;

        if (useTransaction)
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(normalizedConnectionString, true, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            (connection, transaction, dispose) = await ResolveConnectionAsync(normalizedConnectionString, false, cancellationToken).ConfigureAwait(false);
        }

        var dbTypes = ConvertParameterTypes(parameterTypes);
        if (connection == null)
        {
            throw CreateQueryExecutionException(
                "Failed to resolve connection for streaming.",
                query,
                new InvalidOperationException("The SQLite connection could not be resolved."));
        }

        try
        {
            await foreach (var row in base.ExecuteQueryStreamAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
            {
                yield return row;
            }
        }
        finally
        {
            if (dispose)
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
    /// Streams query results asynchronously through a caller-provided mapper.
    /// </summary>
    public virtual IAsyncEnumerable<T> QueryStreamAsync<T>(
        string database,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        return Stream();

        async IAsyncEnumerable<T> Stream()
        {
            var connectionString = BuildOperationalConnectionString(database);

            SqliteConnection? connection = null;
            SqliteTransaction? transaction = null;
            var dispose = false;

            (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteMappedQueryStreamAsync(connection, transaction, query, map, initialize, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                    await connection.DisposeAsync().ConfigureAwait(false);
#else
                    connection.Dispose();
#endif
                }
            }
        }
    }

    /// <summary>
    /// Streams query results from a SQLite connection string through a caller-provided mapper, without building a
    /// <see cref="DataTable"/>.
    /// </summary>
    /// <remarks>
    /// Use this overload for connection options that a database path cannot express, such as <c>Mode=ReadOnly</c> or a
    /// shared in-memory database. Only the current row is held in memory; <paramref name="map"/> must copy what it needs
    /// because the record is reused for the next row.
    /// </remarks>
    public virtual IAsyncEnumerable<T> QueryStreamWithConnectionStringAsync<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));
        var normalizedConnectionString = NormalizeConnectionString(connectionString);

        return Stream();

        async IAsyncEnumerable<T> Stream()
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            var (connection, transaction, dispose) = await ResolveConnectionAsync(normalizedConnectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            try
            {
                await foreach (var row in ExecuteMappedQueryStreamAsync(connection, transaction, query, map, initialize, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    await connection.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
    }
}
#endif
