using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    /// <summary>
    /// Starts a transaction using the default isolation level (<see cref="IsolationLevel.ReadCommitted"/>).
    /// </summary>
    public virtual void BeginTransaction(string host, string database, string username, string password)
        => BeginTransaction(host, database, username, password, IsolationLevel.ReadCommitted);

    /// <summary>
    /// Starts a transaction with the specified <paramref name="isolationLevel"/>.
    /// </summary>
    public virtual void BeginTransaction(string host, string database, string username, string password, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(host, database, username, password);
            MySqlConnection? connection = null;
            MySqlTransaction? transaction = null;
            try
            {
                connection = new MySqlConnection(connectionString);
                connection.Open();
                transaction = connection.BeginTransaction(isolationLevel);
                _transactionConnection = connection;
                _transaction = transaction;
                connection = null;
                transaction = null;
            }
            finally
            {
                transaction?.Dispose();
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously starts a transaction using the default isolation level (<see cref="IsolationLevel.ReadCommitted"/>).
    /// </summary>
    public virtual Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(host, database, username, password, IsolationLevel.ReadCommitted, cancellationToken);

    /// <summary>
    /// Asynchronously starts a transaction with the specified <paramref name="isolationLevel"/>.
    /// </summary>
    public virtual async Task BeginTransactionAsync(string host, string database, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }

        var connectionString = BuildConnectionString(host, database, username, password);

        var connection = new MySqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
        var transaction = connection.BeginTransaction(isolationLevel);
#endif

        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                transaction.Dispose();
                connection.Dispose();
                throw new DbaTransactionException("Transaction already started.");
            }

            _transactionConnection = connection;
            _transaction = transaction;
        }
    }

    /// <summary>
    /// Commits the active transaction.
    /// </summary>
    public virtual void Commit()
    {
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            _transaction.Commit();
            DisposeTransactionLocked();
        }
    }

    /// <summary>
    /// Asynchronously commits the active transaction.
    /// </summary>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        MySqlTransaction? tx;
        MySqlConnection? conn;
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
        }

        try
        {
            await tx!.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
        }
    }

    /// <summary>
    /// Rolls back the active transaction.
    /// </summary>
    public virtual void Rollback()
    {
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            _transaction.Rollback();
            DisposeTransactionLocked();
        }
    }

    /// <summary>
    /// Asynchronously rolls back the active transaction.
    /// </summary>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        MySqlTransaction? tx;
        MySqlConnection? conn;
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
        }

        try
        {
            await tx!.RollbackAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
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
        _transaction?.Dispose();
        _transaction = null;
        _transactionConnection?.Dispose();
        _transactionConnection = null;
    }

    /// <inheritdoc />
    protected override bool IsTransient(Exception ex) =>
        ex is MySqlException mysqlEx &&
        mysqlEx.ErrorCode is MySqlErrorCode.ConnectionCountError
            or MySqlErrorCode.LockDeadlock
            or MySqlErrorCode.LockWaitTimeout
            or MySqlErrorCode.UnableToConnectToHost
            or MySqlErrorCode.XARBDeadlock;

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }

        base.Dispose(disposing);
    }
}
