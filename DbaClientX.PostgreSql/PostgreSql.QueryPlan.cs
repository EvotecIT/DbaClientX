using System.Data;
using System.Runtime.ExceptionServices;
using DBAClientX.QueryPlans;
using Npgsql;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>Obtains one PostgreSQL estimated DML plan without running the explained statement.</summary>
    /// <param name="connectionString">A connection string requiring TLS.</param>
    /// <param name="query">One SELECT, WITH, INSERT, UPDATE, DELETE or MERGE statement.</param>
    /// <param name="parameters">Values for named @parameters or :parameters, bound through Npgsql.</param>
    /// <param name="parameterTypes">Optional existing Npgsql types, including null/array values.</param>
    /// <returns>A native estimated plan with precise output and cost estimates.</returns>
    public virtual DbaQueryPlan ExplainQueryPlan(string connectionString, string query,
        IDictionary<string, object?>? parameters = null, IDictionary<string, NpgsqlDbType>? parameterTypes = null)
        => ExplainQueryPlanAsync(connectionString, query, parameters, parameterTypes).GetAwaiter().GetResult();

    /// <summary>Asynchronously captures a native estimated plan on an owned read-only transaction.</summary>
    /// <param name="connectionString">A connection string requiring TLS.</param>
    /// <param name="query">One DML statement; EXPLAIN/ANALYZE controls and scripts are unsupported.</param>
    /// <param name="parameters">Bound named input values. Caller dictionaries are copied before awaiting.</param>
    /// <param name="parameterTypes">Optional Npgsql types for the bound values.</param>
    /// <param name="cancellationToken">Stops capture and prevents result delivery; cleanup uses an independent deadline.</param>
    /// <returns>A native PostgreSQL plan; rows read and table cardinality remain unknown.</returns>
    /// <remarks>Uses a non-pooled, non-enlisted, non-multiplexed connection, separate from active client/ambient transactions.
    /// Requires standard_conforming_strings=on. Never replays commands or enables ANALYZE. CommandTimeout governs capture;
    /// rollback uses five seconds independently of caller cancellation. Native FullScans/QueryPlanAssert are unsupported.</remarks>
    public virtual async Task<DbaQueryPlan> ExplainQueryPlanAsync(string connectionString, string query,
        IDictionary<string, object?>? parameters = null, IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        var input = ValidateQueryPlanInput(query, parameters, parameterTypes);
        cancellationToken.ThrowIfCancellationRequested();
        var target = new NpgsqlConnectionStringBuilder(connectionString)
        { Pooling = false, Enlist = false, Multiplexing = false, CancellationTimeout = 2000 };
        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        DbaQueryPlan? result = null;
        Exception? failure = null;
        try
        {
            connection = CreateConnection(target.ConnectionString);
            ValidateQueryPlanConnection(connection);
            await AwaitWithCallerCancellationAsync(() => OpenConnectionAsync(connection, cancellationToken),
                cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            transaction = await BeginDbTransactionAsync(connection, IsolationLevel.ReadCommitted, cancellationToken).ConfigureAwait(false);
#else
            // The Framework asset has no async startup API. Native startup is deferred to the next command,
            // so avoid the legacy hook's Task.Yield, which can block synchronous callers on their context.
            cancellationToken.ThrowIfCancellationRequested();
            transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
#endif
            using (var readOnly = new NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction))
            {
                ApplyCommandTimeout(readOnly);
                await ExecuteNonReplayableCommandAsync(() => readOnly.ExecuteNonQueryAsync(cancellationToken), connection,
                    readOnly.CommandText, cancellationToken).ConfigureAwait(false);
            }
            using (var settings = new NpgsqlCommand("SHOW standard_conforming_strings", connection, transaction))
            {
                ApplyCommandTimeout(settings);
                var setting = await ExecuteNonReplayableCommandAsync(() => settings.ExecuteScalarAsync(cancellationToken), connection,
                    settings.CommandText, cancellationToken, NonReplayableCommandKind.Scalar).ConfigureAwait(false);
                if (!string.Equals(setting as string, "on", StringComparison.OrdinalIgnoreCase))
                    throw QueryPlanCaptureContractException.UnsupportedMode("Estimated capture requires standard_conforming_strings=on.");
            }
            var document = await ReadQueryPlanDocumentAsync(connection, transaction, input.Statement, input.Values, input.Types,
                cancellationToken).ConfigureAwait(false);
            try
            {
                result = PostgreSqlQueryPlanParser.Parse(query, document,
                    input.Values?.Count > 0 ? DbaQueryPlanParameterMode.BoundValues : DbaQueryPlanParameterMode.None);
            }
            catch (FormatException) { throw QueryPlanCaptureContractException.InvalidDocument("The native estimated plan document is invalid or unsupported."); }
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception exception)
        {
            failure = IsCallerCancellation(exception, cancellationToken) ? CreateCallerCancellationException(exception, cancellationToken)
                : exception is QueryPlanCaptureContractException contract ? contract.PublicFailure
                : CreateQueryExecutionOrCancellationException("Failed to obtain the estimated query plan.", query, exception, cancellationToken);
        }
        finally
        {
            if (transaction != null)
            {
                try
                {
                    using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    // Both approved Npgsql generations expose native async rollback, including the Framework asset.
                    await transaction.RollbackAsync(cleanupDeadline.Token).ConfigureAwait(false);
                }
                catch (Exception exception) { failure = AddQueryPlanCleanupFailure(failure, exception, "ROLLBACK"); }
                try { await DisposeDbTransactionAsync(transaction).ConfigureAwait(false); }
                catch (Exception exception) { failure = AddQueryPlanCleanupFailure(failure, exception, "DISPOSE TRANSACTION"); }
            }
            if (connection != null)
            {
                try { await DisposeConnectionAsync(connection).ConfigureAwait(false); }
                catch (Exception exception) { failure = AddQueryPlanCleanupFailure(failure, exception, "CLOSE CONNECTION"); }
            }
        }
        if (failure != null) ExceptionDispatchInfo.Capture(failure).Throw();
        cancellationToken.ThrowIfCancellationRequested();
        return result!;
    }

    private Exception AddQueryPlanCleanupFailure(Exception? primary, Exception exception, string operation)
    {
        var cleanup = CreateQueryExecutionException("Failed to clean up the estimated-plan capture.", operation, exception);
        return primary == null ? cleanup : new AggregateException(primary, cleanup);
    }

    private static void ValidateQueryPlanConnection(NpgsqlConnection connection)
    {
        ValidateConnectionString(connection.ConnectionString);
        var effective = new NpgsqlConnectionStringBuilder(connection.ConnectionString);
        if (effective.Pooling || effective.Enlist || effective.Multiplexing
            || effective.CancellationTimeout != 2000 || connection.State != ConnectionState.Closed)
            throw new InvalidOperationException("The plan connection factory must preserve the closed, isolated connection and finite cancellation settings.");
    }

    private async Task<string> ReadQueryPlanDocumentAsync(NpgsqlConnection connection, NpgsqlTransaction transaction,
        string statement, IDictionary<string, object?>? values, IDictionary<string, NpgsqlDbType>? types, CancellationToken token)
    {
        using var command = new NpgsqlCommand("EXPLAIN (ANALYZE FALSE, VERBOSE TRUE, COSTS TRUE, FORMAT JSON) " + statement, connection, transaction);
        ApplyCommandTimeout(command);
        AddParameters(command, values, ConvertParameterTypes(types));
        using var cancellation = token.Register(() =>
        {
            try { command.Cancel(); }
            catch (NpgsqlException) { }
            catch (InvalidOperationException) { }
            catch (TimeoutException) { }
        });
        using var reader = await ExecuteNonReplayableCommandAsync(() => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, token),
            connection, statement, token, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
        string? document = null;
        do
        {
            while (await AwaitWithCallerCancellationAsync(() => reader.ReadAsync(token), token).ConfigureAwait(false))
            {
                if (reader.FieldCount != 1 || document != null) throw QueryPlanCaptureContractException.InvalidDocument("Expected exactly one native plan document.");
                using var text = reader.GetTextReader(0);
                var buffer = new char[4096];
                var builder = new StringBuilder();
                int count;
                while ((count = await AwaitWithCallerCancellationAsync(() => text.ReadAsync(buffer, 0, buffer.Length), token).ConfigureAwait(false)) != 0)
                {
                    if (builder.Length > PostgreSqlQueryPlanParser.MaximumDocumentCharacters - count)
                        throw QueryPlanCaptureContractException.InvalidDocument("The native plan document exceeds the supported size.");
                    builder.Append(buffer, 0, count);
                }
                document = builder.ToString();
            }
        } while (await AwaitWithCallerCancellationAsync(() => reader.NextResultAsync(token), token).ConfigureAwait(false));
        return document ?? throw QueryPlanCaptureContractException.InvalidDocument("PostgreSQL did not return an estimated statement plan.");
    }
}
