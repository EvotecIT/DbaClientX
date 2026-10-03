using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>Opens a SQLite query as an owned, forward-only streaming reader.</summary>
    /// <remarks>
    /// Dispose the returned reader after the consumer, including OfficeIMO.Data.Arrow, has finished reading.
    /// </remarks>
    public virtual Task<DbaDataReader> QueryReaderAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
        => OpenQueryReaderAsync(
            BuildOperationalConnectionString(database),
            query,
            parameters,
            useTransaction,
            cancellationToken,
            parameterTypes,
            parameterDirections);

    /// <summary>Opens a SQLite connection-string query as an owned, forward-only streaming reader.</summary>
    public virtual Task<DbaDataReader> QueryReaderWithConnectionStringAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
        => OpenQueryReaderAsync(
            NormalizeConnectionString(connectionString),
            query,
            parameters,
            useTransaction,
            cancellationToken,
            parameterTypes,
            parameterDirections);

    private async Task<DbaDataReader> OpenQueryReaderAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters,
        bool useTransaction,
        CancellationToken cancellationToken,
        IDictionary<string, SqliteType>? parameterTypes,
        IDictionary<string, ParameterDirection>? parameterDirections)
    {
        ValidateCommandText(query);
        SqliteConnection? connection = null;
        SqliteCommand? command = null;
        CancellationTokenRegistration interrupt = default;
        var dispose = false;
        try
        {
            var resolved = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            (connection, var transaction, dispose) = resolved;
            // The opening token stays attached to an owned connection until the reader closes: canceling it stops
            // the statement while it is opened and while its rows are read.
            interrupt = RegisterOwnedStatementInterrupt(connection, dispose, cancellationToken, query);
            command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, ConvertParameterTypes(parameterTypes), parameterDirections);
            ApplyCommandTimeout(command);
            var reader = await ExecuteReaderWithDiagnosticsAsync(
                () => AwaitWithCallerCancellationAsync(
                    () => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken),
                    cancellationToken),
                connection,
                transaction,
                query,
                cancellationToken).ConfigureAwait(false);
            var lease = interrupt;
            return new DbaDataReader(
                reader,
                command,
                connection,
                dispose,
                // The lease is only registered on an owned connection, whose disposal runs in a finally block even
                // when closing the provider reader throws; end the lease there before the handle closes.
                resource =>
                {
                    lease.Dispose();
                    resource.Dispose();
                },
                () =>
                {
                    lease.Dispose();
                    UpdateOutputParameters(command, parameters);
                },
                resource =>
                {
                    lease.Dispose();
                    return DisposeSQLiteConnectionAsync((SqliteConnection)resource);
                },
                afterReaderDisposedAsync: null,
                // A row read with another token can still be stopped by the opening token's interrupt.
                consumptionExceptionFactory: (exception, token) => CreateQueryExecutionOrCancellationException(
                    "Failed while consuming query reader.",
                    query,
                    exception,
                    token.IsCancellationRequested || !cancellationToken.IsCancellationRequested ? token : cancellationToken));
        }
        catch (Exception ex)
        {
            interrupt.Dispose();
            command?.Dispose();
            if (connection != null && dispose)
            {
                await DisposeOwnedResourceAsync(connection, true, DisposeSQLiteConnectionAsync).ConfigureAwait(false);
            }
            if (IsCallerCancellation(ex, cancellationToken)) throw;
            throw CreateQueryExecutionOrCancellationException("Failed to open query reader.", query, ex, cancellationToken);
        }
    }
}
