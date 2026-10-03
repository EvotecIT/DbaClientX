using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    private async Task<SqlConnection> OpenRecoveryConnectionAsync(string connectionString, CancellationToken token, bool? pooling = null)
    {
        try
        {
            var builder = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master", Enlist = false };
            if (pooling.HasValue) builder.Pooling = pooling.Value;
            var (connection, _, _) = await ResolveConnectionAsync(builder.ConnectionString, false, token).ConfigureAwait(false);
            return connection;
        }
        catch (System.Exception exception) when (!IsCallerCancellation(exception, token))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to open the recovery connection.",
                "OPEN CONNECTION", exception, token);
        }
    }

    private async Task<int> ExecuteRecoveryNonQueryAsync(SqlCommand command, CancellationToken token)
    {
        try
        {
            return await ExecuteNonReplayableCommandAsync(() => command.ExecuteNonQueryAsync(token),
                command.Connection!, command.CommandText, token, countRows: static rows => rows >= 0 ? rows : null).ConfigureAwait(false);
        }
        catch (System.Exception exception) when (!IsCallerCancellation(exception, token))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to execute the recovery command.",
                command.CommandText, exception, token);
        }
    }

    private async Task<object?> ExecuteRecoveryScalarAsync(SqlCommand command, CancellationToken token)
    {
        try
        {
            return await ExecuteNonReplayableCommandAsync(() => command.ExecuteScalarAsync(token),
                command.Connection!, command.CommandText, token, NonReplayableCommandKind.Scalar).ConfigureAwait(false);
        }
        catch (System.Exception exception) when (!IsCallerCancellation(exception, token))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to read recovery metadata.",
                command.CommandText, exception, token);
        }
    }

    private async Task<SqlDataReader> ExecuteRecoveryReaderAsync(SqlCommand command, CancellationToken token)
    {
        try
        {
            return await ExecuteNonReplayableCommandAsync(() => command.ExecuteReaderAsync(token),
                command.Connection!, command.CommandText, token, NonReplayableCommandKind.ReaderOpen).ConfigureAwait(false);
        }
        catch (System.Exception exception) when (!IsCallerCancellation(exception, token))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to read recovery metadata.",
                command.CommandText, exception, token);
        }
    }

    private async Task<bool> ReadRecoveryRowAsync(SqlDataReader reader, string commandText, CancellationToken token)
    {
        try { return await AwaitWithCallerCancellationAsync(() => reader.ReadAsync(token), token).ConfigureAwait(false); }
        catch (System.Exception exception) when (!IsCallerCancellation(exception, token))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to read recovery metadata.", commandText, exception, token);
        }
    }

    private async Task<bool> NextRecoveryResultAsync(SqlDataReader reader, string commandText, CancellationToken token)
    {
        try { return await AwaitWithCallerCancellationAsync(() => reader.NextResultAsync(token), token).ConfigureAwait(false); }
        catch (System.Exception exception) when (!IsCallerCancellation(exception, token))
        {
            throw CreateQueryExecutionOrCancellationException("Failed to read recovery metadata.", commandText, exception, token);
        }
    }
}
