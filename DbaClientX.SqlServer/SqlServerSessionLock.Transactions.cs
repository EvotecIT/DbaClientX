using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public sealed partial class SqlServerSessionLock
{
    /// <summary>
    /// Executes and commits database work on the original session holding the application lock.
    /// Commands are serialized and never retried or reconnected. Any failure after entering the
    /// operation invalidates and closes this lock session, rolling back uncommitted database work.
    /// </summary>
    /// <remarks>
    /// The callback must await its commands and must not alter the database, session locks,
    /// connection or transaction lifetime through SQL. Do not call this lock's other methods
    /// from the callback. Only commands executed through the supplied scope are protected;
    /// external operations and other connections are not fenced. A commit or post-commit failure
    /// can have an uncertain durable result: reconcile it, never automatically replay the callback.
    /// </remarks>
    /// <param name="operation">Database work using the callback-scoped transaction.</param>
    /// <param name="isolationLevel">SQL transaction isolation level.</param>
    /// <param name="commandTimeoutSeconds">Finite command timeout between 1 and 86400 seconds.</param>
    /// <param name="cancellationToken">Cancels admission, commands or commit.</param>
    /// <returns>A task completing only after commit and a final original-session lock check.</returns>
    public async Task ExecuteInTransactionAsync(
        Func<SqlServerSessionTransaction, CancellationToken, Task> operation,
        IsolationLevel isolationLevel = IsolationLevel.Serializable,
        int commandTimeoutSeconds = 30,
        CancellationToken cancellationToken = default)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));
        if (commandTimeoutSeconds is < 1 or > 86400)
            throw new ArgumentOutOfRangeException(nameof(commandTimeoutSeconds));
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        SqlTransaction? transaction = null;
        SqlServerSessionTransaction? scope = null;
        try
        {
            if (_disposed) throw new ObjectDisposedException(nameof(SqlServerSessionLock));
            if (_lost) throw new InvalidOperationException("The SQL Server session lock was lost.");
            try
            {
                await DemandHeldCoreAsync(null, cancellationToken).ConfigureAwait(false);
#if NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                transaction = (SqlTransaction)await _connection.BeginTransactionAsync(
                    isolationLevel, cancellationToken).ConfigureAwait(false);
#else
                cancellationToken.ThrowIfCancellationRequested();
                transaction = _connection.BeginTransaction(isolationLevel);
#endif
                scope = new SqlServerSessionTransaction(_connection, transaction,
                    ct => DemandHeldCoreAsync(transaction, ct), commandTimeoutSeconds);
                await operation(scope, cancellationToken).ConfigureAwait(false);
                if (await scope.CloseScopeAsync().ConfigureAwait(false))
                    throw new InvalidOperationException("A locked SQL transaction command failed; commit is forbidden.");
                cancellationToken.ThrowIfCancellationRequested();
                await DemandHeldCoreAsync(transaction, cancellationToken).ConfigureAwait(false);
#if NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
                transaction.Commit();
#endif
                await DemandHeldCoreAsync(null, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                _lost = true;
                // Closing the non-pooled session is also the rollback boundary on failure.
                _connection.Dispose();
                throw;
            }
        }
        finally
        {
            try
            {
                if (scope != null) await scope.CloseScopeAsync().ConfigureAwait(false);
                transaction?.Dispose();
            }
            finally
            {
                _gate.Release();
            }
        }
    }
}
