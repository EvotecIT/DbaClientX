using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    private CommandRetryMode _commandRetryMode;

    /// <summary>
    /// Gets or sets the replay policy for query, scalar, mapped, reader and nonquery commands.
    /// The default is <see cref="DBAClientX.CommandRetryMode.Never"/>.
    /// </summary>
    /// <remarks>
    /// Returning rows does not make SQL read-only: a batch, stored procedure or RETURNING clause can write.
    /// Use <see cref="DBAClientX.CommandRetryMode.ReplaySafe"/> only on a client whose commands and callbacks
    /// are all safe to repeat after partial success. Commands in managed or ambient transactions are never
    /// retried individually; the caller must roll back and decide whether the complete transaction can be repeated.
    /// Providers can also exclude native transactions started by SQL. Connection establishment uses its separate provider retry policy.
    /// </remarks>
    public CommandRetryMode CommandRetryMode
    {
        get { lock (_syncRoot) { return _commandRetryMode; } }
        set
        {
            if (value is not (DBAClientX.CommandRetryMode.Never or DBAClientX.CommandRetryMode.ReplaySafe))
                throw new ArgumentOutOfRangeException(nameof(value));
            lock (_syncRoot) { _commandRetryMode = value; }
        }
    }

    /// <summary>Executes a command under its replay policy, without replaying an active transaction.</summary>
    /// <typeparam name="T">The command result type.</typeparam>
    /// <param name="operation">One complete command attempt.</param>
    /// <param name="connection">The open connection used by the command.</param>
    /// <param name="transaction">The transaction the command enlists in, if any.</param>
    /// <param name="returnsResults">Whether this is a query, scalar or reader operation.</param>
    /// <returns>The completed command result.</returns>
    protected T ExecuteCommandWithRetry<T>(Func<T> operation, DbConnection connection, DbTransaction? transaction, bool returnsResults = true)
        => CanRetryCommand(connection, transaction, returnsResults)
            ? TransientRetry.Run(operation,
                exception => CanRetryCommand(connection, transaction, returnsResults) && IsTransient(exception),
                CreateTransientRetryOptions())
            : operation();

    /// <summary>Asynchronously executes a command under its replay policy.</summary>
    /// <typeparam name="T">The command result type.</typeparam>
    /// <param name="operation">One complete command attempt.</param>
    /// <param name="connection">The open connection used by the command.</param>
    /// <param name="transaction">The transaction the command enlists in, if any.</param>
    /// <param name="cancellationToken">Token that stops execution and retry delays.</param>
    /// <param name="returnsResults">Whether this is a query, scalar or reader operation.</param>
    /// <returns>The completed command result.</returns>
    protected Task<T> ExecuteCommandWithRetryAsync<T>(
        Func<Task<T>> operation,
        DbConnection connection,
        DbTransaction? transaction,
        CancellationToken cancellationToken = default,
        bool returnsResults = true)
    {
        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<T>(cancellationToken);
        return CanRetryCommand(connection, transaction, returnsResults)
            ? TransientRetry.RunAsync(_ => operation(),
                exception => CanRetryCommand(connection, transaction, returnsResults) && IsTransient(exception),
                CreateTransientRetryOptions(), cancellationToken: cancellationToken)
            : operation();
    }

    /// <summary>Determines whether a command is eligible for replay, including provider-native transaction state.</summary>
    /// <param name="connection">The open connection used by the command.</param>
    /// <param name="transaction">The managed transaction, if any.</param>
    /// <param name="returnsResults">Whether this is a query, scalar or reader operation.</param>
    /// <returns>Whether the client policy permits replay outside a transaction.</returns>
    protected virtual bool CanRetryCommand(DbConnection connection, DbTransaction? transaction, bool returnsResults)
    {
        lock (_syncRoot)
        {
            return transaction == null && System.Transactions.Transaction.Current == null &&
                (_commandRetryMode == DBAClientX.CommandRetryMode.ReplaySafe ||
                 (!returnsResults && _retryNonQueryOperations));
        }
    }
}
