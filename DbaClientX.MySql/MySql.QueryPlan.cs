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
        var transactionAttempted = false;
        var callerAbortRequested = 0;
        CancellationTokenRegistration callerAbort = default;
        MySqlCommand? cancellableCommand = null;
        DbaQueryPlan? result = null;
        Exception? failure = null;
        try
        {
            connection = CreateConnection(target.ConnectionString);
            ValidateQueryPlanConnection(connection);
            await AwaitWithCallerCancellationAsync(() => OpenConnectionAsync(connection, cancellationToken),
                cancellationToken).ConfigureAwait(false);
            callerAbort = cancellationToken.Register(() =>
            {
                Interlocked.Exchange(ref callerAbortRequested, 1);
                // Native KILL QUERY clears a healthy server's lock wait. CancellationTimeout bounds its separate
                // connection; closing the owned connection also settles I/O when the server cannot answer.
                var command = Volatile.Read(ref cancellableCommand);
                if (command != null)
                {
                    try { command.Cancel(); }
                    catch (MySqlException) { }
                    catch (InvalidOperationException) { }
                    catch (TimeoutException) { }
                }
                AbortQueryPlanConnection(connection);
            });
            // Native BeginTransactionAsync uses raw I/O without command timeouts. Use the cancellable command
            // pipeline for this owned transaction, including partial startup and independent rollback.
            transactionAttempted = true;
            await ExecuteQueryPlanControlAsync(connection,
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; START TRANSACTION READ ONLY", cancellationToken).ConfigureAwait(false);
            MySqlQueryPlanFormat format;
            using (var settings = new MySqlCommand("SELECT @@SESSION.sql_mode, VERSION()", connection))
            {
                settings.CommandTimeout = nativeCaptureTimeout;
                ApplyCommandTimeout(settings);
                Volatile.Write(ref cancellableCommand, settings);
                using var reader = await ExecuteNonReplayableCommandAsync(() => settings.ExecuteReaderAsync(CancellationToken.None),
                    connection, settings.CommandText, cancellationToken, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
                if (!await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
                    throw QueryPlanCaptureContractException.InvalidDocument("The server did not report its native product and SQL mode.");
                var modes = reader.GetString(0).Split(',');
                if (modes.Any(mode => mode.Equals("NO_BACKSLASH_ESCAPES", StringComparison.OrdinalIgnoreCase)
                    || mode.Equals("ANSI_QUOTES", StringComparison.OrdinalIgnoreCase)))
                    throw QueryPlanCaptureContractException.UnsupportedMode("Estimated capture requires SQL mode without NO_BACKSLASH_ESCAPES or ANSI_QUOTES.");
                format = reader.GetString(1).IndexOf("MariaDB", StringComparison.OrdinalIgnoreCase) >= 0
                    ? MySqlQueryPlanFormat.MariaDbJson : MySqlQueryPlanFormat.MySqlJsonV1;
            }
            Volatile.Write(ref cancellableCommand, null);
            var document = await ReadQueryPlanDocumentAsync(connection, input.Statement, input.Values, input.Types,
                nativeCaptureTimeout, command => Volatile.Write(ref cancellableCommand, command), cancellationToken).ConfigureAwait(false);
            try
            {
                result = MySqlQueryPlanParser.Parse(query, document, format,
                    input.Values?.Count > 0 ? DbaQueryPlanParameterMode.BoundValues : DbaQueryPlanParameterMode.None);
            }
            catch (FormatException) { throw QueryPlanCaptureContractException.InvalidDocument("The native estimated plan document is invalid or unsupported."); }
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception exception)
        {
            failure = Volatile.Read(ref callerAbortRequested) != 0 && cancellationToken.IsCancellationRequested
                ? CreateCallerCancellationException(exception, cancellationToken)
                : exception is MySqlException { Number: 1792 }
                ? new NotSupportedException("Native estimated capture supports only statements permitted in a read-only transaction.")
                : IsCallerCancellation(exception, cancellationToken) ? CreateCallerCancellationException(exception, cancellationToken)
                : exception is QueryPlanCaptureContractException contract ? contract.PublicFailure
                : CreateQueryExecutionOrCancellationException("Failed to obtain the estimated query plan.", query, exception, cancellationToken);
        }
        finally
        {
            // Stop caller aborts before independent cleanup. Late caller cancellation still blocks delivery below.
            callerAbort.Dispose();
            if (transactionAttempted && connection?.State == ConnectionState.Open)
            {
                try
                {
                    await ExecuteQueryPlanControlAsync(connection, "ROLLBACK", CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception exception) { failure = AddQueryPlanCleanupFailure(failure, exception, "ROLLBACK"); }
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

    private async Task ExecuteQueryPlanControlAsync(MySqlConnection connection, string sql, CancellationToken callerToken)
    {
        using var deadline = new CancellationTokenSource();
        deadline.CancelAfter(TimeSpan.FromSeconds(5));
        var abortRequested = 0;
        using var abort = deadline.Token.Register(() =>
        {
            Interlocked.Exchange(ref abortRequested, 1);
            AbortQueryPlanConnection(connection);
        });
        // The owned watchdog supplies the deadline; native timeout/KILL recovery would require another live server connection.
        using var startup = new MySqlCommand(sql, connection) { CommandTimeout = 0 };
        try
        {
            await ExecuteNonReplayableCommandAsync(() => startup.ExecuteNonQueryAsync(CancellationToken.None), connection,
                startup.CommandText, callerToken).ConfigureAwait(false);
            callerToken.ThrowIfCancellationRequested();
            if (deadline.IsCancellationRequested) throw new TimeoutException("Estimated-plan transaction control exceeded five seconds.");
        }
        catch (Exception exception) when (Volatile.Read(ref abortRequested) != 0)
        {
            if (callerToken.IsCancellationRequested) throw CreateCallerCancellationException(exception, callerToken);
            throw new TimeoutException("Estimated-plan transaction control exceeded five seconds.");
        }
    }

    private static void AbortQueryPlanConnection(MySqlConnection connection)
    {
        // Closing this non-pooled connection settles pending I/O; the command task is still awaited before returning.
        try { connection.Close(); }
        catch (MySqlException) { }
        catch (InvalidOperationException) { }
        catch (IOException) { }
        catch (NotSupportedException) { } // TLS can reject the reader drain when another read is pending.
        catch (System.Net.Sockets.SocketException) { }
        catch (TimeoutException) { }
    }

    private static void ValidateQueryPlanConnection(MySqlConnection connection)
    {
        ValidateConnectionString(connection.ConnectionString);
        var effective = new MySqlConnectionStringBuilder(connection.ConnectionString);
        if (effective.Pooling || effective.AutoEnlist || effective.AllowUserVariables
            || effective.CancellationTimeout != 2 || effective.DefaultCommandTimeout != 5 || connection.State != ConnectionState.Closed)
            throw new InvalidOperationException("The plan connection factory must preserve the closed, isolated connection and finite cancellation settings.");
    }

    private async Task<string> ReadQueryPlanDocumentAsync(MySqlConnection connection,
        string statement, IDictionary<string, object?>? values, IDictionary<string, MySqlDbType>? types, int nativeCaptureTimeout,
        Action<MySqlCommand?> registerCommand, CancellationToken token)
    {
        using var command = new MySqlCommand("EXPLAIN FORMAT=JSON " + statement, connection);
        command.CommandTimeout = nativeCaptureTimeout;
        ApplyCommandTimeout(command);
        AddParameters(command, values, ConvertParameterTypes(types));
        registerCommand(command);
        try
        {
            using var reader = await ExecuteNonReplayableCommandAsync(() => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, CancellationToken.None),
                connection, statement, token, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
            string? document = null;
            do
            {
                while (await AwaitWithCallerCancellationAsync(() => reader.ReadAsync(CancellationToken.None), token).ConfigureAwait(false))
                {
                    if (reader.FieldCount != 1 || document != null) throw QueryPlanCaptureContractException.InvalidDocument("Expected exactly one native plan document.");
                    using var text = reader.GetTextReader(0);
                    var buffer = new char[4096];
                    var builder = new StringBuilder();
                    int count;
                    while ((count = await AwaitWithCallerCancellationAsync(() => text.ReadAsync(buffer, 0, buffer.Length), token).ConfigureAwait(false)) != 0)
                    {
                        if (builder.Length > MySqlQueryPlanParser.MaximumDocumentCharacters - count)
                            throw QueryPlanCaptureContractException.InvalidDocument("The native plan document exceeds the supported size.");
                        builder.Append(buffer, 0, count);
                    }
                    document = builder.ToString();
                }
            } while (await AwaitWithCallerCancellationAsync(() => reader.NextResultAsync(CancellationToken.None), token).ConfigureAwait(false));
            return document ?? throw QueryPlanCaptureContractException.InvalidDocument("MySQL/MariaDB did not return an estimated statement plan.");
        }
        finally { registerCommand(null); }
    }
}
