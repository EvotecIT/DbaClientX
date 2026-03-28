using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Starts a new transaction using a dedicated <see cref="SqliteConnection"/> targeting the provided database.
    /// </summary>
    public virtual void BeginTransaction(string database)
    {
        lock (_syncRoot)
        {
            EnsureTransactionStartAllowed(_transaction, _transactionInitializing);
            var connectionString = BuildOperationalConnectionString(database);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);
            SqliteConnection? connection = null;
            SqliteTransaction? transaction = null;
            try
            {
                connection = new SqliteConnection(connectionString);
                connection.Open();
                ApplyBusyTimeout(connection);
                transaction = connection.BeginTransaction();
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
    /// Starts a new transaction using the provider default isolation semantics.
    /// </summary>
    public virtual void BeginTransaction(string database, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            EnsureTransactionStartAllowed(_transaction, _transactionInitializing);
            var connectionString = BuildOperationalConnectionString(database);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);
            SqliteConnection? connection = null;
            SqliteTransaction? transaction = null;
            try
            {
                connection = new SqliteConnection(connectionString);
                connection.Open();
                ApplyBusyTimeout(connection);
                transaction = connection.BeginTransaction(isolationLevel);
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
    /// Asynchronously starts a new transaction using a dedicated <see cref="SqliteConnection"/> targeting the provided database.
    /// </summary>
    public virtual async Task BeginTransactionAsync(string database, CancellationToken cancellationToken = default)
    {
        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        try
        {
            lock (_syncRoot)
            {
                ReserveTransactionStart(_transaction, ref _transactionInitializing);
            }

            var connectionString = BuildOperationalConnectionString(database);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);

            connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ApplyBusyTimeoutAsync(connection, busyTimeoutMs: null, cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
            transaction = connection.BeginTransaction();
#endif

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
    /// Asynchronously starts a new transaction using the provider default isolation semantics.
    /// </summary>
    public virtual Task BeginTransactionAsync(string database, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        => BeginTransactionAsyncCore(database, isolationLevel, cancellationToken);

    /// <summary>
    /// Runs the provided callback inside a SQLite transaction and commits on success.
    /// </summary>
    public virtual void RunInTransaction(
        string database,
        Action<SQLite> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        RunInTransaction(
            database,
            client =>
            {
                operation(client);
                return true;
            },
            isolationLevel);
    }

    /// <summary>
    /// Runs the provided callback inside a SQLite transaction and commits on success.
    /// </summary>
    public virtual TResult RunInTransaction<TResult>(
        string database,
        Func<SQLite, TResult> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return ExecuteInTransaction(
            () => BeginTransaction(database, isolationLevel),
            () => operation(this),
            Commit,
            Rollback,
            () => IsInTransaction);
    }

    /// <summary>
    /// Runs the provided asynchronous callback inside a SQLite transaction and commits on success.
    /// </summary>
    public virtual Task RunInTransactionAsync(
        string database,
        Func<SQLite, CancellationToken, Task> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return RunInTransactionAsync(
            database,
            async (client, token) =>
            {
                await operation(client, token).ConfigureAwait(false);
                return true;
            },
            isolationLevel,
            cancellationToken);
    }

    /// <summary>
    /// Runs the provided asynchronous callback inside a SQLite transaction and commits on success.
    /// </summary>
    public virtual async Task<TResult> RunInTransactionAsync<TResult>(
        string database,
        Func<SQLite, CancellationToken, Task<TResult>> operation,
        IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
        CancellationToken cancellationToken = default)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        return await ExecuteInTransactionAsync(
            token => BeginTransactionAsync(database, isolationLevel, token),
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
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
            tx!.Commit();
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
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            await Task.Yield();
            tx!.Commit();
#endif
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
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
            tx!.Rollback();
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
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
            await Task.Yield();
            tx!.Rollback();
#endif
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
        SqliteTransaction? transaction;
        SqliteConnection? connection;
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
    protected virtual void TryRollbackDbTransactionOnDispose(SqliteTransaction? transaction)
    {
        if (transaction == null)
        {
            return;
        }

        try
        {
            transaction.Rollback();
        }
        catch
        {
        }
    }

    /// <summary>
    /// Attempts to asynchronously roll back an active provider transaction during disposal, ignoring rollback failures so cleanup can continue.
    /// </summary>
    protected virtual async Task TryRollbackDbTransactionOnDisposeAsync(SqliteTransaction? transaction)
    {
        if (transaction == null)
        {
            return;
        }

        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
#else
            await Task.Yield();
            transaction.Rollback();
#endif
        }
        catch
        {
        }
    }

    /// <summary>
    /// Disposes SQLite transaction resources created for the current operation.
    /// </summary>
    protected virtual void DisposeTransactionResources(SqliteTransaction? transaction, SqliteConnection? connection)
        => DisposeResourcePair(transaction, DisposeDbTransaction, connection, DisposeDbConnection);

    /// <summary>
    /// Asynchronously disposes SQLite transaction resources created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeTransactionResourcesAsync(SqliteTransaction? transaction, SqliteConnection? connection)
        => DisposeResourcePairAsync(transaction, DisposeDbTransactionAsync, connection, DisposeDbConnectionAsync);

    /// <inheritdoc />
    protected override bool IsTransient(Exception ex) =>
        ex is SqliteException sqliteEx &&
        sqliteEx.SqliteErrorCode is 5 or 6;

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

    private async Task BeginTransactionAsyncCore(string database, IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
        SqliteConnection? connection = null;
        SqliteTransaction? transaction = null;
        try
        {
            lock (_syncRoot)
            {
                ReserveTransactionStart(_transaction, ref _transactionInitializing);
            }

            var connectionString = BuildOperationalConnectionString(database);
            var normalizedConnectionString = NormalizeConnectionString(connectionString);

            connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            await ApplyBusyTimeoutAsync(connection, busyTimeoutMs: null, cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            transaction = (SqliteTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
            transaction = connection.BeginTransaction(isolationLevel);
#endif

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
    /// Disposes a provider transaction created for the current operation.
    /// </summary>
    protected virtual void DisposeDbTransaction(SqliteTransaction transaction) => transaction.Dispose();

    /// <summary>
    /// Asynchronously disposes a provider transaction created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeDbTransactionAsync(SqliteTransaction transaction)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return transaction.DisposeAsync();
#else
        transaction.Dispose();
        return default;
#endif
    }

    /// <summary>
    /// Disposes a provider connection created for the current operation.
    /// </summary>
    protected virtual void DisposeDbConnection(SqliteConnection connection) => connection.Dispose();

    /// <summary>
    /// Asynchronously disposes a provider connection created for the current operation.
    /// </summary>
    protected virtual ValueTask DisposeDbConnectionAsync(SqliteConnection connection)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return connection.DisposeAsync();
#else
        connection.Dispose();
        return default;
#endif
    }
}
