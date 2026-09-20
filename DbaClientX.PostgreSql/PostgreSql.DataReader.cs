using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>Opens a PostgreSQL query as an owned, forward-only streaming reader.</summary>
    /// <remarks>
    /// Dispose the returned reader after the consumer, including OfficeIMO.Data.Arrow, has finished reading.
    /// </remarks>
    public virtual async Task<DbaDataReader> QueryReaderAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        ValidateConnectionString(connectionString);
        ValidateCommandText(query);
        NpgsqlConnection? connection = null;
        NpgsqlCommand? command = null;
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
                resource => DisposeConnection((NpgsqlConnection)resource),
                () => UpdateOutputParameters(command, parameters),
                resource => DisposeConnectionAsync((NpgsqlConnection)resource));
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
