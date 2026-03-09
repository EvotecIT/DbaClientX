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
            if (_transaction != null || _transactionInitializing)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(host, database, username, password);
            NpgsqlConnection? connection = null;
            NpgsqlTransaction? transaction = null;
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
                if (_transaction != null || _transactionInitializing)
                {
                    throw new DbaTransactionException("Transaction already started.");
                }

                _transactionInitializing = true;
            }

            var connectionString = BuildConnectionString(host, database, username, password);

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
    /// Commits the currently active transaction.
    /// </summary>
    public virtual void Commit()
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
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
    /// Asynchronously commits the currently active transaction.
    /// </summary>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
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
    /// Rolls back the currently active transaction.
    /// </summary>
    public virtual void Rollback()
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
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
    /// Asynchronously rolls back the currently active transaction.
    /// </summary>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
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
    /// Disposes transaction resources created for the current operation.
    /// </summary>
    protected virtual void DisposeTransactionResources(NpgsqlTransaction? transaction, NpgsqlConnection? connection)
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
}
