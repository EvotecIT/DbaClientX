using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    /// <summary>
    /// Attempts to roll back an active transaction after an operation failure and rethrows the original exception.
    /// </summary>
    protected static void HandleTransactionFailure(Exception originalException, Action rollback, Func<bool> hasActiveTransaction)
    {
        var hadActiveTransaction = hasActiveTransaction();
        Exception? rollbackException = null;
        try
        {
            rollback();
        }
        catch (DbaTransactionException) when (!hadActiveTransaction)
        {
            rollbackException = null;
        }
        catch (Exception ex)
        {
            rollbackException = ex;
        }

        if (rollbackException != null)
        {
            throw new AggregateException("Transaction operation failed and rollback also failed.", originalException, rollbackException);
        }

        ExceptionDispatchInfo.Capture(originalException).Throw();
    }

    /// <summary>
    /// Attempts to roll back an active transaction after an asynchronous operation failure and rethrows the original exception.
    /// </summary>
    protected static async Task HandleTransactionFailureAsync(
        Exception originalException,
        Func<CancellationToken, Task> rollbackAsync,
        Func<bool> hasActiveTransaction,
        CancellationToken cancellationToken)
    {
        var hadActiveTransaction = hasActiveTransaction();
        Exception? rollbackException = null;
        try
        {
            await rollbackAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (DbaTransactionException) when (!hadActiveTransaction)
        {
            rollbackException = null;
        }
        catch (Exception ex)
        {
            rollbackException = ex;
        }

        if (rollbackException != null)
        {
            throw new AggregateException("Transaction operation failed and rollback also failed.", originalException, rollbackException);
        }

        ExceptionDispatchInfo.Capture(originalException).Throw();
    }

    /// <summary>
    /// Executes an operation inside an already-configured transaction lifecycle.
    /// </summary>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="beginTransaction">Callback that starts the transaction.</param>
    /// <param name="operation">Callback executed inside the transaction.</param>
    /// <param name="commitTransaction">Callback that commits the transaction.</param>
    /// <param name="rollbackTransaction">Callback that rolls back the transaction on failure.</param>
    /// <param name="hasActiveTransaction">Callback that indicates whether a transaction is still active.</param>
    protected static TResult ExecuteInTransaction<TResult>(
        Action beginTransaction,
        Func<TResult> operation,
        Action commitTransaction,
        Action rollbackTransaction,
        Func<bool> hasActiveTransaction)
    {
        beginTransaction();
        try
        {
            var result = operation();
            commitTransaction();
            return result;
        }
        catch (Exception ex)
        {
            HandleTransactionFailure(ex, rollbackTransaction, hasActiveTransaction);
            throw;
        }
    }

    /// <summary>
    /// Executes an asynchronous operation inside an already-configured transaction lifecycle.
    /// </summary>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="beginTransactionAsync">Callback that starts the transaction.</param>
    /// <param name="operationAsync">Callback executed inside the transaction.</param>
    /// <param name="commitTransactionAsync">Callback that commits the transaction.</param>
    /// <param name="rollbackTransactionAsync">Callback that rolls back the transaction on failure.</param>
    /// <param name="hasActiveTransaction">Callback that indicates whether a transaction is still active.</param>
    /// <param name="cancellationToken">Cancellation token for the async workflow.</param>
    protected static async Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<CancellationToken, Task> beginTransactionAsync,
        Func<CancellationToken, Task<TResult>> operationAsync,
        Func<CancellationToken, Task> commitTransactionAsync,
        Func<CancellationToken, Task> rollbackTransactionAsync,
        Func<bool> hasActiveTransaction,
        CancellationToken cancellationToken)
    {
        await beginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var result = await operationAsync(cancellationToken).ConfigureAwait(false);
            await commitTransactionAsync(cancellationToken).ConfigureAwait(false);
            return result;
        }
        catch (Exception ex)
        {
            await HandleTransactionFailureAsync(ex, rollbackTransactionAsync, hasActiveTransaction, cancellationToken).ConfigureAwait(false);
            throw;
        }
    }
}
