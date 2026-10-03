using System.Runtime.ExceptionServices;
using DBAClientX.QueryPlans;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Obtains SQL Server's estimated plan for one DML statement without executing that statement.</summary>
    /// <param name="connectionString">The target database connection string.</param>
    /// <param name="query">A SELECT, INSERT, UPDATE, DELETE or MERGE statement, without SHOWPLAN controls.</param>
    /// <param name="parameterTypes">Scalar declarations for every @parameter. They create unassigned local variables, not value-sensitive bound plans.</param>
    /// <returns>A plan with native identities, access operators and precise optimizer estimates.</returns>
    /// <remarks>Uses an owned non-pooled, non-enlisted connection. SHOWPLAN permission is required in every referenced database.
    /// QueryPlanAssert and FullScans remain SQLite-specific; inspect ScanOperations and Estimates for native SQL Server plans.</remarks>
    public virtual DbaQueryPlan ExplainQueryPlan(string connectionString, string query,
        IDictionary<string, SqlServerQueryPlanParameter>? parameterTypes = null)
        => ExplainQueryPlanAsync(connectionString, query, parameterTypes).GetAwaiter().GetResult();

    /// <summary>Asynchronously obtains an estimated plan without executing the explained DML statement.</summary>
    /// <param name="connectionString">The target database connection string.</param>
    /// <param name="query">One SELECT, INSERT, UPDATE, DELETE or MERGE statement, without session controls.</param>
    /// <param name="parameterTypes">Types of unassigned local variables used for generic estimates; parameter values are never interpolated.</param>
    /// <param name="cancellationToken">Stops capture; cleanup uses its own finite timeout.</param>
    /// <returns>A structured native estimated plan.</returns>
    /// <remarks>Ignores the client's active transaction and ambient enlistment. Connections are not pooled. Native commands never replay.
    /// The provider's CommandTimeout applies to capture. Cleanup has a five-second timeout and is attempted even after cancellation.
    /// Documents are limited to 16,777,216 characters and 4096 operators. Unsupported/no-plan statements fail explicitly.</remarks>
    public virtual async Task<DbaQueryPlan> ExplainQueryPlanAsync(string connectionString, string query,
        IDictionary<string, SqlServerQueryPlanParameter>? parameterTypes = null, CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(connectionString);
        var (batch, mode) = BuildQueryPlanBatch(query, parameterTypes);
        cancellationToken.ThrowIfCancellationRequested();
        var target = new SqlConnectionStringBuilder(connectionString) { Pooling = false, Enlist = false };
        SqlConnection? connection = null;
        DbaQueryPlan? result = null;
        Exception? failure = null;
        try
        {
            connection = await OpenQueryPlanConnectionAsync(target.ConnectionString, cancellationToken).ConfigureAwait(false);
            await SetQueryPlanModeAsync(connection, enabled: true, cancellationToken).ConfigureAwait(false);
            result = SqlServerQueryPlanParser.Parse(query,
                await ReadQueryPlanDocumentAsync(connection, batch, cancellationToken).ConfigureAwait(false), mode);
        }
        catch (Exception exception)
        {
            failure = IsCallerCancellation(exception, cancellationToken) || exception is FormatException or System.Xml.XmlException ? exception
                : CreateQueryExecutionOrCancellationException("Failed to obtain the estimated query plan.", query, exception, cancellationToken);
        }
        finally
        {
            if (connection != null)
            {
                try { await SetQueryPlanModeAsync(connection, enabled: false, CancellationToken.None).ConfigureAwait(false); }
                catch (Exception exception)
                {
                    var cleanup = CreateQueryExecutionException("Failed to clear estimated-plan session state.", "SET SHOWPLAN_XML OFF", exception);
                    failure = failure == null ? cleanup : new AggregateException(failure, cleanup);
                }
                try { await DisposeConnectionAsync(connection).ConfigureAwait(false); }
                catch (Exception exception)
                {
                    var cleanup = CreateQueryExecutionException("Failed to close the estimated-plan connection.", "CLOSE CONNECTION", exception);
                    failure = failure == null ? cleanup : new AggregateException(failure, cleanup);
                }
            }
        }
        if (failure != null) ExceptionDispatchInfo.Capture(failure).Throw();
        return result!;
    }

    private Task<SqlConnection> OpenQueryPlanConnectionAsync(string connectionString, CancellationToken token)
        => ExecuteConnectionOpenWithRetryAsync(async () =>
        {
            var connection = CreateConnection(connectionString);
            try
            {
                var effective = new SqlConnectionStringBuilder(connection.ConnectionString);
                if (effective.Pooling || effective.Enlist || connection.State != System.Data.ConnectionState.Closed)
                    throw new InvalidOperationException("The estimated-plan connection factory must return a closed, non-pooled, non-enlisted connection.");
                await AwaitWithCallerCancellationAsync(() => OpenConnectionAsync(connection, token), token).ConfigureAwait(false);
                return connection;
            }
            catch
            {
                await DisposeOwnedResourceAsync(connection, ownsResource: true, DisposeConnectionAsync).ConfigureAwait(false);
                throw;
            }
        }, token);

    private async Task SetQueryPlanModeAsync(SqlConnection connection, bool enabled, CancellationToken token)
    {
        using var command = new SqlCommand(enabled ? "SET SHOWPLAN_XML ON" : "SET SHOWPLAN_XML OFF", connection);
        if (enabled) ApplyCommandTimeout(command);
        else command.CommandTimeout = 5;
        await ExecuteNonReplayableCommandAsync(() => command.ExecuteNonQueryAsync(token), connection, command.CommandText, token).ConfigureAwait(false);
    }

    private async Task<string> ReadQueryPlanDocumentAsync(SqlConnection connection, string batch, CancellationToken token)
    {
        using var command = new SqlCommand(batch, connection);
        ApplyCommandTimeout(command);
        using var cancellation = token.Register(() =>
        {
            try { command.Cancel(); }
            catch (SqlException) { }
            catch (InvalidOperationException) { }
        });
        using var reader = await ExecuteNonReplayableCommandAsync(() => command.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, token),
            connection, batch, token, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
        string? document = null;
        do
        {
            while (await AwaitWithCallerCancellationAsync(() => reader.ReadAsync(token), token).ConfigureAwait(false))
            {
                if (reader.FieldCount != 1 || document != null) throw new FormatException("Expected exactly one native plan document.");
                using var text = reader.GetTextReader(0);
                var buffer = new char[4096];
                var builder = new System.Text.StringBuilder();
                int count;
                while ((count = await AwaitWithCallerCancellationAsync(() => text.ReadAsync(buffer, 0, buffer.Length), token).ConfigureAwait(false)) != 0)
                {
                    if (builder.Length > SqlServerQueryPlanParser.MaximumDocumentCharacters - count)
                        throw new FormatException("The native plan document exceeds the supported size.");
                    builder.Append(buffer, 0, count);
                }
                document = builder.ToString();
            }
        } while (await AwaitWithCallerCancellationAsync(() => reader.NextResultAsync(token), token).ConfigureAwait(false));
        return document ?? throw new FormatException("SQL Server did not return an estimated statement plan.");
    }
}
