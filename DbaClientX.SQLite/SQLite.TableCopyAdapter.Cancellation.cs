using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter
{
    /// <summary>
    /// Runs a command on <paramref name="connection"/> so that canceling <paramref name="cancellationToken"/> stops a
    /// statement that is running, and reports the stop as <see cref="OperationCanceledException"/> for that token.
    /// </summary>
    /// <remarks>
    /// Read sessions run only SELECT statements on their shared connection, and an interrupted SELECT does not end the
    /// read transaction, so the session connection is interrupted like an owned one.
    /// </remarks>
    private static async Task<T> RunInterruptibleAsync<T>(
        SqliteConnection connection,
        Func<Task<T>> operation,
        CancellationToken cancellationToken)
    {
        using (SQLite.RegisterStatementInterrupt(connection, cancellationToken))
        {
            try
            {
                return await operation().ConfigureAwait(false);
            }
            catch (SqliteException exception) when (
                cancellationToken.IsCancellationRequested &&
                exception.SqliteErrorCode == SQLite.SqliteInterruptErrorCode)
            {
                // The engine treats only OperationCanceledException as cancellation; the interrupt error carries no
                // other information the caller needs.
                throw new OperationCanceledException(cancellationToken);
            }
        }
    }
}
