using System.Data;
using System.Runtime.ExceptionServices;
using DBAClientX.QueryPlans;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    /// <summary>Captures one native estimated SELECT/WITH plan without executing the explained statement.</summary>
    /// <param name="connectionString">Explicit TCPS connection details, including the caller's wallet settings.</param>
    /// <param name="query">One SELECT/WITH statement without scripts or local PL/SQL functions.</param>
    /// <param name="parameters">Named :parameter values, bound through the existing Oracle provider rules.</param>
    /// <param name="parameterTypes">Optional Oracle types, including typed null values.</param>
    /// <returns>Native operator IDs, object names and nullable output/cost estimates.</returns>
    public virtual DbaQueryPlan ExplainQueryPlan(string connectionString, string query,
        IDictionary<string, object?>? parameters = null, IDictionary<string, OracleDbType>? parameterTypes = null)
        => ExplainQueryPlanAsync(connectionString, query, parameters, parameterTypes).GetAwaiter().GetResult();

    /// <summary>Asynchronously captures an estimated plan into Oracle's session-owned temporary plan table.</summary>
    /// <param name="connectionString">Explicit TCPS connection details. Server DN matching is enforced.</param>
    /// <param name="query">One SELECT/WITH statement, at most 1Mi characters; writes and scripts are unsupported.</param>
    /// <param name="parameters">Unquoted named :parameter values; dictionaries are copied before awaiting.</param>
    /// <param name="parameterTypes">Optional provider types for bound values.</param>
    /// <param name="cancellationToken">Cancels capture and prevents late delivery; rollback has an independent deadline.</param>
    /// <returns>A native Oracle PLAN_TABLE plan; rows read, table cardinality and database remain unknown.</returns>
    /// <remarks>Owns a non-pooled, non-enlisted connection, independent of client/ambient transactions. Requires
    /// Oracle's SYS.PLAN_TABLE$ to be temporary; creates/drops no tables and rolls back all owned rows before delivery.
    /// Never replays commands. Capture follows CommandTimeout/native timeout; rollback uses five seconds independently.
    /// EXPLAIN does not provide a cached runtime plan or guarantee bind peeking; date-bind conversions may be unsupported.
    /// Parsing/optimization can acquire metadata locks or invoke optimizer functions, whose external effects cannot be rolled back.
    /// Native FullScans/QueryPlanAssert are unsupported. Limits are 4096 operators and 4096 characters per native text field.</remarks>
    public virtual async Task<DbaQueryPlan> ExplainQueryPlanAsync(string connectionString, string query,
        IDictionary<string, object?>? parameters = null, IDictionary<string, OracleDbType>? parameterTypes = null,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        var input = ValidateQueryPlanInput(query, parameters, parameterTypes);
        cancellationToken.ThrowIfCancellationRequested();
        var target = new OracleConnectionStringBuilder(connectionString) { Pooling = false, Enlist = "false" };
        ValidateQueryPlanTransport(target.DataSource);
        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        DbaQueryPlan? result = null;
        Exception? failure = null;
        try
        {
            connection = CreateConnection(target.ConnectionString);
            if (connection.State != ConnectionState.Closed || !new OracleConnectionStringBuilder(connection.ConnectionString).EquivalentTo(target))
                throw new InvalidOperationException("The plan connection factory must preserve the closed, isolated connection and supplied TLS settings.");
            connection.SSLServerDNMatch = true;
            await AwaitWithCallerCancellationAsync(() => OpenConnectionAsync(connection, cancellationToken), cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            // ReadCommitted transaction startup is local ODP.NET state; avoid the Framework Task.Yield host hook.
            transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
            using (var check = new OracleCommand("SELECT TEMPORARY FROM ALL_TABLES WHERE OWNER='SYS' AND TABLE_NAME='PLAN_TABLE$'", connection))
            {
                check.Transaction = transaction;
                ApplyCommandTimeout(check);
                using var cancel = RegisterQueryPlanCancel(check, cancellationToken);
                object? temporary = await ExecuteNonReplayableCommandAsync(() => check.ExecuteScalarAsync(cancellationToken),
                    connection, check.CommandText, cancellationToken, NonReplayableCommandKind.Scalar).ConfigureAwait(false);
                if (!string.Equals(temporary as string, "Y", StringComparison.Ordinal))
                    throw new NotSupportedException("Estimated capture requires Oracle's session-owned temporary SYS.PLAN_TABLE$.");
            }
            string identity = "DBAX" + Guid.NewGuid().ToString("N").Substring(0, 26);
            using (var explain = new OracleCommand("EXPLAIN PLAN SET STATEMENT_ID='" + identity + "' INTO SYS.PLAN_TABLE$ FOR " + input.Statement, connection))
            {
                explain.Transaction = transaction;
                ApplyCommandTimeout(explain);
                AddParameters(explain, input.Values, ConvertParameterTypes(input.Types));
                using var cancel = RegisterQueryPlanCancel(explain, cancellationToken);
                await ExecuteNonReplayableCommandAsync(() => explain.ExecuteNonQueryAsync(cancellationToken), connection,
                    query, cancellationToken).ConfigureAwait(false);
            }
            using var rows = await ReadQueryPlanRowsAsync(connection, transaction, identity, cancellationToken).ConfigureAwait(false);
            result = OracleQueryPlanParser.Parse(query, rows, input.Values?.Count > 0 ? DbaQueryPlanParameterMode.BoundValues : DbaQueryPlanParameterMode.None);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception error)
        {
            failure = IsCallerCancellation(error, cancellationToken) ? CreateCallerCancellationException(error, cancellationToken)
                : error is FormatException or NotSupportedException ? error
                : CreateQueryExecutionOrCancellationException("Failed to obtain the estimated query plan.", query, error, cancellationToken);
        }
        finally
        {
            if (transaction != null && connection?.State == ConnectionState.Open)
            {
                try { await RollbackQueryPlanAsync(connection, transaction).ConfigureAwait(false); }
                catch (Exception error) { failure = AddQueryPlanCleanupFailure(failure, error, "ROLLBACK"); }
            }
            if (transaction != null)
            {
                try { await DisposeDbTransactionAsync(transaction).ConfigureAwait(false); }
                catch (Exception error) { failure = AddQueryPlanCleanupFailure(failure, error, "DISPOSE TRANSACTION"); }
            }
            if (connection != null)
            {
                try { await DisposeConnectionAsync(connection).ConfigureAwait(false); }
                catch (Exception error) { failure = AddQueryPlanCleanupFailure(failure, error, "CLOSE CONNECTION"); }
            }
        }
        if (failure != null) ExceptionDispatchInfo.Capture(failure).Throw();
        cancellationToken.ThrowIfCancellationRequested();
        return result!;
    }

    private Exception AddQueryPlanCleanupFailure(Exception? primary, Exception error, string operation)
    {
        var cleanup = CreateQueryExecutionException("Failed to clean up the estimated-plan capture.", operation, error);
        return primary == null ? cleanup : new AggregateException(primary, cleanup);
    }

    private static CancellationTokenRegistration RegisterQueryPlanCancel(OracleCommand command, CancellationToken token)
        => token.Register(() =>
        {
            try { command.Cancel(); }
            catch (OracleException) { }
            catch (InvalidOperationException) { }
        });

    private async Task RollbackQueryPlanAsync(OracleConnection connection, OracleTransaction transaction)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        // A native command gives the Framework lane the same async/timeout behavior without posting to a caller context.
        using var command = new OracleCommand("ROLLBACK", connection) { Transaction = transaction, CommandTimeout = 5 };
        using var cancel = RegisterQueryPlanCancel(command, deadline.Token);
        await ExecuteNonReplayableCommandAsync(() => command.ExecuteNonQueryAsync(deadline.Token), connection,
            command.CommandText, deadline.Token).ConfigureAwait(false);
        deadline.Token.ThrowIfCancellationRequested();
    }

    private async Task<DataTable> ReadQueryPlanRowsAsync(OracleConnection connection, OracleTransaction transaction, string identity, CancellationToken token)
    {
        const string sql = "SELECT PLAN_ID,ID,PARENT_ID,OPERATION,OPTIONS,OBJECT_OWNER,OBJECT_NAME,OBJECT_TYPE,OBJECT_ALIAS,CARDINALITY,COST "
            + "FROM SYS.PLAN_TABLE$ WHERE STATEMENT_ID=:identity ORDER BY ID";
        using var command = new OracleCommand(sql, connection) { Transaction = transaction, BindByName = true };
        command.Parameters.Add("identity", OracleDbType.Varchar2).Value = identity;
        ApplyCommandTimeout(command);
        using var cancel = RegisterQueryPlanCancel(command, token);
        using var reader = await ExecuteNonReplayableCommandAsync(() => command.ExecuteReaderAsync(token), connection,
            sql, token, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
        var table = new DataTable();
        for (int column = 0; column < reader.FieldCount; column++)
            table.Columns.Add(reader.GetName(column), column == 0 ? typeof(decimal) : column is 1 or 2 ? typeof(int) : column is 9 or 10 ? typeof(double) : typeof(string));
        while (await AwaitWithCallerCancellationAsync(() => reader.ReadAsync(token), token).ConfigureAwait(false))
        {
            if (table.Rows.Count >= OracleQueryPlanParser.MaximumOperators) throw new FormatException("The native plan exceeds 4096 operators.");
            var values = new object[reader.FieldCount];
            for (int column = 0; column < reader.FieldCount; column++)
            {
                if (reader.IsDBNull(column)) values[column] = DBNull.Value;
                else if (column == 0) values[column] = reader.GetDecimal(column);
                else if (column is 1 or 2) values[column] = reader.GetInt32(column);
                else if (column is 9 or 10) values[column] = reader.GetDouble(column);
                else
                {
                    string value = reader.GetString(column);
                    if (value.Length > 4096) throw new FormatException("Native operator text exceeds 4096 characters.");
                    values[column] = value;
                }
            }
            table.Rows.Add(values);
        }
        return table;
    }
}
