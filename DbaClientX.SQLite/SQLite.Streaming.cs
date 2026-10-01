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
    /// Canceling the token interrupts a running statement unless it runs inside the client's transaction.
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
        var (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
        var interrupt = RegisterOwnedStatementInterrupt(connection, dispose, cancellationToken);
        try
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            await foreach (var row in base.ExecuteQueryStreamAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
            {
                yield return row;
            }
        }
        finally
        {
            interrupt.Dispose();
            await DisposeOwnedResourceAsync(connection, dispose, DisposeSQLiteConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Streams query results asynchronously from a SQLite connection string, yielding one <see cref="DataRow"/> at a time.
    /// </summary>
    /// <remarks>
    /// SQLite columns can mix storage classes across rows. When a later row needs a different column type (for example a
    /// TEXT value after INTEGER values), rows from that point are created from a new <see cref="DataTable"/> with the adapted
    /// schema, so do not assume every streamed row shares the schema of the first row's <see cref="DataRow.Table"/>.
    /// Canceling the token interrupts a running statement unless it runs inside the client's transaction.
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
        var (connection, transaction, dispose) = await ResolveConnectionAsync(normalizedConnectionString, useTransaction, cancellationToken).ConfigureAwait(false);
        var interrupt = RegisterOwnedStatementInterrupt(connection, dispose, cancellationToken);
        try
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            await foreach (var row in base.ExecuteQueryStreamAsync(connection, transaction, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
            {
                yield return row;
            }
        }
        finally
        {
            interrupt.Dispose();
            await DisposeOwnedResourceAsync(connection, dispose, DisposeSQLiteConnectionAsync).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Streams query results asynchronously through a caller-provided mapper.
    /// </summary>
    /// <remarks>
    /// Canceling the token interrupts a running statement unless it runs inside the client's transaction.
    /// </remarks>
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

        return StreamMappedAsync(BuildOperationalConnectionString(database), useTransaction, query, map, initialize, parameters, parameterTypes, parameterDirections, busyTimeoutMs: null, cancellationToken);
    }

    /// <summary>
    /// Streams query results from a SQLite connection string through a caller-provided mapper, without building a
    /// <see cref="DataTable"/>.
    /// </summary>
    /// <remarks>
    /// Use this overload for connection options that a database path cannot express, such as <c>Mode=ReadOnly</c> or a
    /// shared in-memory database. Only the current row is held in memory; <paramref name="map"/> must copy what it needs
    /// because the record is reused for the next row. Canceling the token interrupts a running statement unless it runs
    /// inside the client's transaction.
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

        return StreamMappedAsync(normalizedConnectionString, useTransaction, query, map, initialize, parameters, parameterTypes, parameterDirections, busyTimeoutMs: null, cancellationToken);
    }

    /// <summary>
    /// Streams query results from a read-only connection through a caller-provided mapper, without building a
    /// <see cref="DataTable"/>.
    /// </summary>
    /// <typeparam name="T">The type produced by the row mapping callback.</typeparam>
    /// <param name="database">Path to an existing SQLite database file; a missing file is reported, not created.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="map">Maps the current row. The record is reused for the next row, so copy what is needed.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Stops the stream; a running statement is interrupted.</param>
    /// <param name="busyTimeoutMs">Optional busy timeout in milliseconds.</param>
    /// <param name="parameterTypes">Optional provider-specific parameter types.</param>
    /// <param name="initialize">Optional callback invoked once after the reader opens and before the first row is read.</param>
    /// <returns>The mapped rows, one at a time.</returns>
    /// <remarks>
    /// The connection is opened with <c>Mode=ReadOnly</c>, so a statement that writes fails with <c>SQLITE_READONLY</c>.
    /// This is the streaming counterpart of <see cref="QueryReadOnlyAsListAsync{T}"/>: only the current row is held in
    /// memory, and the read does not take a write connection. Like the other streaming APIs, a provider failure is
    /// thrown as the provider's exception (for example <see cref="SqliteException"/>) rather than wrapped in
    /// <see cref="DbaQueryExecutionException"/>; cancellation is reported as <see cref="OperationCanceledException"/>.
    /// </remarks>
    public virtual IAsyncEnumerable<T> QueryReadOnlyStreamAsync<T>(
        string database,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        int? busyTimeoutMs = null,
        IDictionary<string, SqliteType>? parameterTypes = null,
        Action<IDataRecord>? initialize = null)
    {
        ValidateCommandText(query);
        if (map == null) throw new ArgumentNullException(nameof(map));

        return StreamMappedAsync(BuildOperationalConnectionString(database, readOnly: true), useTransaction: false, query, map, initialize, parameters, parameterTypes, parameterDirections: null, busyTimeoutMs, cancellationToken);
    }

    private async IAsyncEnumerable<T> StreamMappedAsync<T>(
        string connectionString,
        bool useTransaction,
        string query,
        Func<IDataRecord, T> map,
        Action<IDataRecord>? initialize,
        IDictionary<string, object?>? parameters,
        IDictionary<string, SqliteType>? parameterTypes,
        IDictionary<string, ParameterDirection>? parameterDirections,
        int? busyTimeoutMs,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var dbTypes = ConvertParameterTypes(parameterTypes);
        var (connection, transaction, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken, busyTimeoutMs).ConfigureAwait(false);
        var interrupt = RegisterOwnedStatementInterrupt(connection, dispose, cancellationToken);
        try
        {
            await foreach (var row in ExecuteMappedQueryStreamAsync(connection, transaction, query, map, initialize, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
            {
                yield return row;
            }
        }
        finally
        {
            interrupt.Dispose();
            await DisposeOwnedResourceAsync(connection, dispose, DisposeSQLiteConnectionAsync).ConfigureAwait(false);
        }
    }
}
#endif
