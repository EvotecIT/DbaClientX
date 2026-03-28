using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    /// <summary>
    /// Begins a transaction using the Oracle connection derived from the provided credentials.
    /// </summary>
    public virtual void BeginTransaction(string host, string serviceName, string username, string password)
        => BeginTransaction(host, serviceName, username, password, IsolationLevel.ReadCommitted);

    /// <summary>
    /// Begins a transaction using the Oracle connection derived from the provided credentials and isolation level.
    /// </summary>
    public virtual void BeginTransaction(string host, string serviceName, string username, string password, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            EnsureTransactionStartAllowed(_transaction, _transactionInitializing);
            var connectionString = BuildConnectionString(host, serviceName, username, password);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);
            OracleConnection? connection = null;
            OracleTransaction? transaction = null;
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
    /// Asynchronously begins a transaction using the Oracle connection derived from the provided credentials.
    /// </summary>
    public virtual Task BeginTransactionAsync(string host, string serviceName, string username, string password, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(host, serviceName, username, password, IsolationLevel.ReadCommitted, cancellationToken);

    /// <summary>
    /// Asynchronously begins a transaction using the Oracle connection derived from the provided credentials and isolation level.
    /// </summary>
    public virtual async Task BeginTransactionAsync(string host, string serviceName, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        try
        {
            lock (_syncRoot)
            {
                ReserveTransactionStart(_transaction, ref _transactionInitializing);
            }

            var connectionString = BuildConnectionString(host, serviceName, username, password);
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
    /// Runs the provided callback inside an Oracle transaction and commits on success.
    /// </summary>
    public virtual void RunInTransaction(
        string host,
        string serviceName,
        string username,
        string password,
        Action<Oracle> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        RunInTransaction(
            host,
            serviceName,
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
    /// Runs the provided callback inside an Oracle transaction and commits on success.
    /// </summary>
    public virtual TResult RunInTransaction<TResult>(
        string host,
        string serviceName,
        string username,
        string password,
        Func<Oracle, TResult> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return ExecuteInTransaction(
            () => BeginTransaction(host, serviceName, username, password, isolationLevel),
            () => operation(this),
            Commit,
            Rollback,
            () => IsInTransaction);
    }

    /// <summary>
    /// Runs the provided asynchronous callback inside an Oracle transaction and commits on success.
    /// </summary>
    public virtual Task RunInTransactionAsync(
        string host,
        string serviceName,
        string username,
        string password,
        Func<Oracle, CancellationToken, Task> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return RunInTransactionAsync(
            host,
            serviceName,
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
    /// Runs the provided asynchronous callback inside an Oracle transaction and commits on success.
    /// </summary>
    public virtual async Task<TResult> RunInTransactionAsync<TResult>(
        string host,
        string serviceName,
        string username,
        string password,
        Func<Oracle, CancellationToken, Task<TResult>> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return await ExecuteInTransactionAsync(
            token => BeginTransactionAsync(host, serviceName, username, password, isolationLevel, token),
            token => operation(this, token),
            CommitAsync,
            RollbackAsync,
            () => IsInTransaction,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Commits the active transaction.
    /// </summary>
    public virtual void Commit()
    {
        OracleTransaction? tx;
        OracleConnection? conn;
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
    /// Asynchronously commits the active transaction.
    /// </summary>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        OracleTransaction? tx;
        OracleConnection? conn;
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
    /// Rolls back the active transaction.
    /// </summary>
    public virtual void Rollback()
    {
        OracleTransaction? tx;
        OracleConnection? conn;
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
    /// Asynchronously rolls back the active transaction.
    /// </summary>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        OracleTransaction? tx;
        OracleConnection? conn;
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
        OracleTransaction? transaction;
        OracleConnection? connection;
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
    protected virtual void TryRollbackDbTransactionOnDispose(OracleTransaction? transaction)
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
    protected virtual async Task TryRollbackDbTransactionOnDisposeAsync(OracleTransaction? transaction)
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
    protected virtual OracleTransaction BeginDbTransaction(OracleConnection connection, IsolationLevel isolationLevel) => connection.BeginTransaction(isolationLevel);

    /// <summary>
    /// Asynchronously begins a provider transaction on the supplied open connection.
    /// </summary>
    protected virtual async Task<OracleTransaction> BeginDbTransactionAsync(OracleConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return (OracleTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
        await Task.Yield();
        return connection.BeginTransaction(isolationLevel);
#endif
    }

    /// <summary>
    /// Commits the supplied provider transaction.
    /// </summary>
    protected virtual void CommitDbTransaction(OracleTransaction transaction) => transaction.Commit();

    /// <summary>
    /// Asynchronously commits the supplied provider transaction.
    /// </summary>
    protected virtual async Task CommitDbTransactionAsync(OracleTransaction transaction, CancellationToken cancellationToken)
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
    protected virtual void RollbackDbTransaction(OracleTransaction transaction) => transaction.Rollback();

    /// <summary>
    /// Asynchronously rolls back the supplied provider transaction.
    /// </summary>
    protected virtual async Task RollbackDbTransactionAsync(OracleTransaction transaction, CancellationToken cancellationToken)
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
    protected virtual void DisposeDbTransaction(OracleTransaction transaction) => transaction.Dispose();

    /// <summary>
    /// Asynchronously disposes a provider transaction created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeDbTransactionAsync(OracleTransaction transaction)
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
    protected virtual void DisposeTransactionResources(OracleTransaction? transaction, OracleConnection? connection)
        => DisposeResourcePair(transaction, DisposeDbTransaction, connection, DisposeConnection);

    /// <summary>
    /// Asynchronously disposes transaction resources created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeTransactionResourcesAsync(OracleTransaction? transaction, OracleConnection? connection)
        => DisposeResourcePairAsync(transaction, DisposeDbTransactionAsync, connection, DisposeConnectionAsync);

    /// <inheritdoc />
    protected override bool IsTransient(Exception ex) =>
        ex is OracleException oex && (oex.Number == 12541 || oex.Number == 12545 || oex.Number == 1089 || oex.Number == 3113 || oex.Number == 3114);

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
