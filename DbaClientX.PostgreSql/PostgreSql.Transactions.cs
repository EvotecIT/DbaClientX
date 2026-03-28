using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <summary>
    /// Begins a new transaction using the default isolation level.
    /// </summary>
    public virtual void BeginTransaction(string host, string database, string username, string password)
        => BeginTransaction(host, database, username, password, IsolationLevel.ReadCommitted);

    /// <summary>
    /// Begins a new transaction using the specified isolation level.
    /// </summary>
    public virtual void BeginTransaction(string host, string database, string username, string password, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            EnsureTransactionStartAllowed(_transaction, _transactionInitializing);
            var connectionString = BuildConnectionString(host, database, username, password);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);
            NpgsqlConnection? connection = null;
            NpgsqlTransaction? transaction = null;
            try
            {
                connection = CreateConnection(connectionString);
                OpenConnection(connection);
                transaction = BeginDbTransaction(connection, isolationLevel);
                StoreStartedTransaction(
                    ref _transaction,
                    ref _transactionConnection,
                    ref _transactionConnectionString,
                    ref _transactionInitializing,
                    transaction,
                    connection,
                    normalizedConnectionString);
                connection = null;
                transaction = null;
            }
            finally
            {
                DisposeTransactionResources(transaction, connection);
            }
        }
    }

    /// <summary>
    /// Asynchronously begins a new transaction using the default isolation level.
    /// </summary>
    public virtual Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(host, database, username, password, IsolationLevel.ReadCommitted, cancellationToken);

    /// <summary>
    /// Asynchronously begins a new transaction using the specified isolation level.
    /// </summary>
    public virtual async Task BeginTransactionAsync(string host, string database, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
        try
        {
            lock (_syncRoot)
            {
                ReserveTransactionStart(_transaction, ref _transactionInitializing);
            }

            var connectionString = BuildConnectionString(host, database, username, password);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);

            connection = CreateConnection(connectionString);
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            transaction = await BeginDbTransactionAsync(connection, isolationLevel, cancellationToken).ConfigureAwait(false);

            lock (_syncRoot)
            {
                StoreStartedTransaction(
                    ref _transaction,
                    ref _transactionConnection,
                    ref _transactionConnectionString,
                    ref _transactionInitializing,
                    transaction,
                    connection,
                    normalizedConnectionString);
                connection = null;
                transaction = null;
            }
        }
        catch
        {
            lock (_syncRoot)
            {
                ReleaseTransactionStartReservationIfNeeded(_transaction, ref _transactionInitializing);
            }

            await DisposeTransactionResourcesAsync(transaction, connection).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Runs the provided callback inside a PostgreSQL transaction and commits on success.
    /// </summary>
    public virtual void RunInTransaction(
        string host,
        string database,
        string username,
        string password,
        Action<PostgreSql> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        RunInTransaction(
            host,
            database,
            username,
            password,
            client =>
            {
                operation(client);
                return true;
            },
            isolationLevel);
    }

    /// <summary>
    /// Runs the provided callback inside a PostgreSQL transaction and commits on success.
    /// </summary>
    public virtual TResult RunInTransaction<TResult>(
        string host,
        string database,
        string username,
        string password,
        Func<PostgreSql, TResult> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return ExecuteInTransaction(
            () => BeginTransaction(host, database, username, password, isolationLevel),
            () => operation(this),
            Commit,
            Rollback,
            () => IsInTransaction);
    }

    /// <summary>
    /// Runs the provided asynchronous callback inside a PostgreSQL transaction and commits on success.
    /// </summary>
    public virtual Task RunInTransactionAsync(
        string host,
        string database,
        string username,
        string password,
        Func<PostgreSql, CancellationToken, Task> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return RunInTransactionAsync(
            host,
            database,
            username,
            password,
            async (client, token) =>
            {
                await operation(client, token).ConfigureAwait(false);
                return true;
            },
            isolationLevel,
            cancellationToken);
    }

    /// <summary>
    /// Runs the provided asynchronous callback inside a PostgreSQL transaction and commits on success.
    /// </summary>
    public virtual async Task<TResult> RunInTransactionAsync<TResult>(
        string host,
        string database,
        string username,
        string password,
        Func<PostgreSql, CancellationToken, Task<TResult>> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return await ExecuteInTransactionAsync(
            token => BeginTransactionAsync(host, database, username, password, isolationLevel, token),
            token => operation(this, token),
            CommitAsync,
            RollbackAsync,
            () => IsInTransaction,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Commits the currently active transaction.
    /// </summary>
    public virtual void Commit()
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
        lock (_syncRoot)
        {
            (tx, conn) = DetachTransactionState(
                ref _transaction,
                ref _transactionConnection,
                ref _transactionConnectionString,
                ref _transactionInitializing,
                requireActiveTransaction: true);
        }

        try
        {
            CommitDbTransaction(tx!);
        }
        finally
        {
            DisposeTransactionResources(tx, conn);
        }
    }

    /// <summary>
    /// Asynchronously commits the currently active transaction.
    /// </summary>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
        lock (_syncRoot)
        {
            (tx, conn) = DetachTransactionState(
                ref _transaction,
                ref _transactionConnection,
                ref _transactionConnectionString,
                ref _transactionInitializing,
                requireActiveTransaction: true);
        }

        try
        {
            await CommitDbTransactionAsync(tx!, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await DisposeTransactionResourcesAsync(tx, conn).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Rolls back the currently active transaction.
    /// </summary>
    public virtual void Rollback()
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
        lock (_syncRoot)
        {
            (tx, conn) = DetachTransactionState(
                ref _transaction,
                ref _transactionConnection,
                ref _transactionConnectionString,
                ref _transactionInitializing,
                requireActiveTransaction: true);
        }

        try
        {
            RollbackDbTransaction(tx!);
        }
        finally
        {
            DisposeTransactionResources(tx, conn);
        }
    }

    /// <summary>
    /// Asynchronously rolls back the currently active transaction.
    /// </summary>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
        lock (_syncRoot)
        {
            (tx, conn) = DetachTransactionState(
                ref _transaction,
                ref _transactionConnection,
                ref _transactionConnectionString,
                ref _transactionInitializing,
                requireActiveTransaction: true);
        }

        try
        {
            await RollbackDbTransactionAsync(tx!, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await DisposeTransactionResourcesAsync(tx, conn).ConfigureAwait(false);
        }
    }

    private void DisposeTransaction()
    {
        lock (_syncRoot)
        {
            DisposeTransactionLocked();
        }
    }

    private void DisposeTransactionLocked()
    {
        var (transaction, connection) = DetachTransactionState(
            ref _transaction,
            ref _transactionConnection,
            ref _transactionConnectionString,
            ref _transactionInitializing);

        TryRollbackDbTransactionOnDispose(transaction);
        DisposeTransactionResources(transaction, connection);
    }

    private async ValueTask DisposeTransactionAsync()
    {
        NpgsqlTransaction? transaction;
        NpgsqlConnection? connection;
        lock (_syncRoot)
        {
            (transaction, connection) = DetachTransactionState(
                ref _transaction,
                ref _transactionConnection,
                ref _transactionConnectionString,
                ref _transactionInitializing);
        }

        await TryRollbackDbTransactionOnDisposeAsync(transaction).ConfigureAwait(false);
        await DisposeTransactionResourcesAsync(transaction, connection).ConfigureAwait(false);
    }

    /// <summary>
    /// Attempts to roll back an active provider transaction during disposal, ignoring rollback failures so cleanup can continue.
    /// </summary>
    protected virtual void TryRollbackDbTransactionOnDispose(NpgsqlTransaction? transaction)
    {
        if (transaction == null)
        {
            return;
        }

        try
        {
            RollbackDbTransaction(transaction);
        }
        catch
        {
        }
    }

    /// <summary>
    /// Attempts to asynchronously roll back an active provider transaction during disposal, ignoring rollback failures so cleanup can continue.
    /// </summary>
    protected virtual async Task TryRollbackDbTransactionOnDisposeAsync(NpgsqlTransaction? transaction)
    {
        if (transaction == null)
        {
            return;
        }

        try
        {
            await RollbackDbTransactionAsync(transaction, CancellationToken.None).ConfigureAwait(false);
        }
        catch
        {
        }
    }

    /// <summary>
    /// Begins a provider transaction on the supplied open connection.
    /// </summary>
    protected virtual NpgsqlTransaction BeginDbTransaction(NpgsqlConnection connection, IsolationLevel isolationLevel) => connection.BeginTransaction(isolationLevel);

    /// <summary>
    /// Asynchronously begins a provider transaction on the supplied open connection.
    /// </summary>
    protected virtual async Task<NpgsqlTransaction> BeginDbTransactionAsync(NpgsqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return (NpgsqlTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
        await Task.Yield();
        return connection.BeginTransaction(isolationLevel);
#endif
    }

    /// <summary>
    /// Commits the supplied provider transaction.
    /// </summary>
    protected virtual void CommitDbTransaction(NpgsqlTransaction transaction) => transaction.Commit();

    /// <summary>
    /// Asynchronously commits the supplied provider transaction.
    /// </summary>
    protected virtual async Task CommitDbTransactionAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
        await Task.Yield();
        transaction.Commit();
#endif
    }

    /// <summary>
    /// Rolls back the supplied provider transaction.
    /// </summary>
    protected virtual void RollbackDbTransaction(NpgsqlTransaction transaction) => transaction.Rollback();

    /// <summary>
    /// Asynchronously rolls back the supplied provider transaction.
    /// </summary>
    protected virtual async Task RollbackDbTransactionAsync(NpgsqlTransaction transaction, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
        await Task.Yield();
        transaction.Rollback();
#endif
    }

    /// <summary>
    /// Disposes a provider transaction created for the current operation.
    /// </summary>
    protected virtual void DisposeDbTransaction(NpgsqlTransaction transaction) => transaction.Dispose();

    /// <summary>
    /// Asynchronously disposes a provider transaction created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeDbTransactionAsync(NpgsqlTransaction transaction)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return transaction.DisposeAsync();
#else
        transaction.Dispose();
        return default;
#endif
    }

    /// <summary>
    /// Disposes transaction resources created for the current operation.
    /// </summary>
    protected virtual void DisposeTransactionResources(NpgsqlTransaction? transaction, NpgsqlConnection? connection)
        => DisposeResourcePair(transaction, DisposeDbTransaction, connection, DisposeConnection);

    /// <summary>
    /// Asynchronously disposes transaction resources created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeTransactionResourcesAsync(NpgsqlTransaction? transaction, NpgsqlConnection? connection)
        => DisposeResourcePairAsync(transaction, DisposeDbTransactionAsync, connection, DisposeConnectionAsync);

    /// <inheritdoc />
    protected override bool IsTransient(Exception ex) =>
        ex is PostgresException pgEx &&
        pgEx.SqlState is "40001" or "40P01" or "55P03" or "53300" or "55006";

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }

        base.Dispose(disposing);
    }

    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore()
    {
        await DisposeTransactionAsync().ConfigureAwait(false);
    }
}
