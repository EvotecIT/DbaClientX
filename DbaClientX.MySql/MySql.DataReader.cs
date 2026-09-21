using System.Data;
using System.Data.Common;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    /// <summary>Opens a MySQL query as an owned, forward-only streaming reader.</summary>
    /// <remarks>
    /// Dispose the returned reader after the consumer, including OfficeIMO.Data.Arrow, has finished reading.
    /// </remarks>
    public virtual async Task<DbaDataReader> QueryReaderAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);
        MySqlConnection? connection = null;
        MySqlCommand? command = null;
        var dispose = false;
        try
        {
            var resolved = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            (connection, var transaction, dispose) = resolved;
            command = connection.CreateCommand();
            command.CommandText = query;
            command.Transaction = transaction;
            AddParameters(command, parameters, ConvertParameterTypes(parameterTypes), parameterDirections);
            ApplyCommandTimeout(command);
            var reader = await ExecuteWithRetryAsync(
                () => AwaitWithCallerCancellationAsync(
                    () => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken),
                    cancellationToken),
                cancellationToken).ConfigureAwait(false);
            return new DbaDataReader(
                reader,
                command,
                connection,
                dispose,
                resource => DisposeConnection((MySqlConnection)resource),
                () => UpdateOutputParameters(command, parameters),
                resource => DisposeConnectionAsync((MySqlConnection)resource),
                afterReaderDisposedAsync: null,
                consumptionExceptionFactory: (exception, token) => CreateQueryExecutionOrCancellationException(
                    "Failed while consuming query reader.", query, exception, token));
        }
        catch (Exception ex)
        {
            command?.Dispose();
            if (connection != null && dispose)
            {
                await DisposeOwnedResourceAsync(connection, true, DisposeConnectionAsync).ConfigureAwait(false);
            }
            if (IsCallerCancellation(ex, cancellationToken)) throw;
            throw CreateQueryExecutionOrCancellationException("Failed to open query reader.", query, ex, cancellationToken);
        }
    }
}
