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
            if (_transaction != null || _transactionInitializing)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(host, serviceName, username, password);
            OracleConnection? connection = null;
            OracleTransaction? transaction = null;
            try
            {
                connection = CreateConnection(connectionString);
                OpenConnection(connection);
                transaction = BeginDbTransaction(connection, isolationLevel);
                _transactionConnection = connection;
                _transaction = transaction;
                _transactionConnectionString = NormalizeConnectionString(connectionString);
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
                if (_transaction != null || _transactionInitializing)
                {
                    throw new DbaTransactionException("Transaction already started.");
                }

                _transactionInitializing = true;
            }

            var connectionString = BuildConnectionString(host, serviceName, username, password);
            connection = CreateConnection(connectionString);
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            transaction = await BeginDbTransactionAsync(connection, isolationLevel, cancellationToken).ConfigureAwait(false);
            lock (_syncRoot)
            {
                if (_transaction != null)
                {
                    _transactionInitializing = false;
                    throw new DbaTransactionException("Transaction already started.");
                }

                _transactionConnection = connection;
                _transaction = transaction;
                _transactionConnectionString = NormalizeConnectionString(connectionString);
                _transactionInitializing = false;
                connection = null;
                transaction = null;
            }
        }
        catch
        {
            lock (_syncRoot)
            {
                if (_transaction == null)
                {
                    _transactionInitializing = false;
                }
            }

            DisposeTransactionResources(transaction, connection);
            throw;
        }
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
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
            _transactionConnectionString = null;
            _transactionInitializing = false;
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
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
            _transactionConnectionString = null;
            _transactionInitializing = false;
        }

        try
        {
            await CommitDbTransactionAsync(tx!, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DisposeTransactionResources(tx, conn);
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
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
            _transactionConnectionString = null;
            _transactionInitializing = false;
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
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }

            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
            _transactionConnectionString = null;
            _transactionInitializing = false;
        }

        try
        {
            await RollbackDbTransactionAsync(tx!, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            DisposeTransactionResources(tx, conn);
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
        if (_transaction != null)
        {
            DisposeDbTransaction(_transaction);
        }
        _transaction = null;
        if (_transactionConnection != null)
        {
            DisposeConnection(_transactionConnection);
        }
        _transactionConnection = null;
        _transactionConnectionString = null;
        _transactionInitializing = false;
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
    /// Disposes transaction resources created for the current operation.
    /// </summary>
    protected virtual void DisposeTransactionResources(OracleTransaction? transaction, OracleConnection? connection)
    {
        if (transaction != null)
        {
            DisposeDbTransaction(transaction);
        }

        if (connection != null)
        {
            DisposeConnection(connection);
        }
    }

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
}
