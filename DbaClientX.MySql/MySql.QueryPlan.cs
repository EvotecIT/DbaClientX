using System.Data;
using System.Runtime.ExceptionServices;
using DBAClientX.QueryPlans;
using MySqlConnector;


namespace DBAClientX;

public partial class MySql
{
    /// <summary>Obtains one MySQL/MariaDB estimated SELECT plan without running the explained statement.</summary>
    /// <param name="connectionString">A connection string requiring TLS.</param>
    /// <param name="query">One SELECT or WITH statement permitted in a native read-only transaction.</param>
    /// <param name="parameters">Values for named @parameters or ?parameters, bound through MySqlConnector.</param>
    /// <param name="parameterTypes">Optional existing MySqlConnector types, including typed null values.</param>
    /// <returns>A native estimated plan with provider-specific row-access estimates.</returns>
    public virtual DbaQueryPlan ExplainQueryPlan(string connectionString, string query,
        IDictionary<string, object?>? parameters = null, IDictionary<string, MySqlDbType>? parameterTypes = null)
        => ExplainQueryPlanAsync(connectionString, query, parameters, parameterTypes).GetAwaiter().GetResult();

    /// <summary>Asynchronously captures a native estimated plan on an owned read-only transaction.</summary>
    /// <param name="connectionString">A connection string requiring TLS.</param>
    /// <param name="query">One SELECT/WITH statement; writes, EXPLAIN/ANALYZE controls and scripts are unsupported.</param>
    /// <param name="parameters">Bound named input values. Caller dictionaries are copied before awaiting.</param>
    /// <param name="parameterTypes">Optional MySqlConnector types for the bound values.</param>
    /// <param name="cancellationToken">Stops capture and prevents result delivery; cleanup uses an independent deadline.</param>
    /// <returns>A native MySQL/MariaDB plan; output and table cardinality remain unknown.</returns>
    /// <remarks>Uses a non-pooled, non-enlisted connection, separate from active client/ambient transactions.
    /// Requires SQL mode without NO_BACKSLASH_ESCAPES or ANSI_QUOTES. Never replays commands or enables ANALYZE. CommandTimeout governs capture;
    /// otherwise the supplied native command timeout applies. Native transaction startup/disposal has a five-second default;
    /// rollback also uses a five-second independent deadline, with two-second native cancellation.
    /// EXPLAIN can acquire metadata locks and invoke optimizer functions; read-only transactions constrain persistent table writes,
    /// not external function effects. Native FullScans/QueryPlanAssert are unsupported.</remarks>
    public virtual async Task<DbaQueryPlan> ExplainQueryPlanAsync(string connectionString, string query,
        IDictionary<string, object?>? parameters = null, IDictionary<string, MySqlDbType>? parameterTypes = null,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        var input = ValidateQueryPlanInput(query, parameters, parameterTypes);
        cancellationToken.ThrowIfCancellationRequested();
        var target = new MySqlConnectionStringBuilder(connectionString)
        { Pooling = false, AutoEnlist = false, AllowUserVariables = false, CancellationTimeout = 2 };
        var nativeCaptureTimeout = checked((int)target.DefaultCommandTimeout);
        target.DefaultCommandTimeout = 5;
        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
        DbaQueryPlan? result = null;
        Exception? failure = null;
        try
        {
            connection = CreateConnection(target.ConnectionString);
            ValidateQueryPlanConnection(connection);
            await AwaitWithCallerCancellationAsync(() => OpenConnectionAsync(connection, cancellationToken),
                cancellationToken).ConfigureAwait(false);
            // MySqlConnector exposes native async read-only startup on Framework and modern runtimes.
            transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted, isReadOnly: true,
                cancellationToken).ConfigureAwait(false);
            MySqlQueryPlanFormat format;
            using (var settings = new MySqlCommand("SELECT @@SESSION.sql_mode, VERSION()", connection, transaction))
            {
                settings.CommandTimeout = nativeCaptureTimeout;
                ApplyCommandTimeout(settings);
                using var reader = await ExecuteNonReplayableCommandAsync(() => settings.ExecuteReaderAsync(cancellationToken),
                    connection, settings.CommandText, cancellationToken, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    throw new FormatException("The server did not report its native product and SQL mode.");
                var modes = reader.GetString(0).Split(',');
                if (modes.Any(mode => mode.Equals("NO_BACKSLASH_ESCAPES", StringComparison.OrdinalIgnoreCase)
                    || mode.Equals("ANSI_QUOTES", StringComparison.OrdinalIgnoreCase)))
                    throw new NotSupportedException("Estimated capture requires SQL mode without NO_BACKSLASH_ESCAPES or ANSI_QUOTES.");
                format = reader.GetString(1).IndexOf("MariaDB", StringComparison.OrdinalIgnoreCase) >= 0
                    ? MySqlQueryPlanFormat.MariaDbJson : MySqlQueryPlanFormat.MySqlJsonV1;
            }
            var document = await ReadQueryPlanDocumentAsync(connection, transaction, input.Statement, input.Values, input.Types,
                nativeCaptureTimeout, cancellationToken).ConfigureAwait(false);
            result = MySqlQueryPlanParser.Parse(query, document, format,
                input.Values?.Count > 0 ? DbaQueryPlanParameterMode.BoundValues : DbaQueryPlanParameterMode.None);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception exception)
        {
            failure = exception is MySqlException { Number: 1792 }
                ? new NotSupportedException("Native estimated capture supports only statements permitted in a read-only transaction.")
                : IsCallerCancellation(exception, cancellationToken) || exception is FormatException or NotSupportedException ? exception
                : CreateQueryExecutionOrCancellationException("Failed to obtain the estimated query plan.", query, exception, cancellationToken);
        }
        finally
        {
            if (transaction != null)
            {
                try
                {
                    using var cleanupDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                    // Both approved MySqlConnector generations expose native async rollback, including the Framework asset.
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

    private static void ValidateQueryPlanConnection(MySqlConnection connection)
    {
        ValidateConnectionString(connection.ConnectionString);
        var effective = new MySqlConnectionStringBuilder(connection.ConnectionString);
        if (effective.Pooling || effective.AutoEnlist || effective.AllowUserVariables
            || effective.CancellationTimeout != 2 || effective.DefaultCommandTimeout != 5 || connection.State != ConnectionState.Closed)
            throw new InvalidOperationException("The plan connection factory must preserve the closed, isolated connection and finite cancellation settings.");
    }

    private async Task<string> ReadQueryPlanDocumentAsync(MySqlConnection connection, MySqlTransaction transaction,
        string statement, IDictionary<string, object?>? values, IDictionary<string, MySqlDbType>? types, int nativeCaptureTimeout, CancellationToken token)
    {
        using var command = new MySqlCommand("EXPLAIN FORMAT=JSON " + statement, connection, transaction);
        command.CommandTimeout = nativeCaptureTimeout;
        ApplyCommandTimeout(command);
        AddParameters(command, values, ConvertParameterTypes(types));
        using var cancellation = token.Register(() =>
        {
            try { command.Cancel(); }
            catch (MySqlException) { }
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
                if (reader.FieldCount != 1 || document != null) throw new FormatException("Expected exactly one native plan document.");
                using var text = reader.GetTextReader(0);
                var buffer = new char[4096];
                var builder = new StringBuilder();
                int count;
                while ((count = await AwaitWithCallerCancellationAsync(() => text.ReadAsync(buffer, 0, buffer.Length), token).ConfigureAwait(false)) != 0)
                {
                    if (builder.Length > MySqlQueryPlanParser.MaximumDocumentCharacters - count)
                        throw new FormatException("The native plan document exceeds the supported size.");
                    builder.Append(buffer, 0, count);
                }
                document = builder.ToString();
            }
        } while (await AwaitWithCallerCancellationAsync(() => reader.NextResultAsync(token), token).ConfigureAwait(false));
        return document ?? throw new FormatException("MySQL/MariaDB did not return an estimated statement plan.");
    }
}

