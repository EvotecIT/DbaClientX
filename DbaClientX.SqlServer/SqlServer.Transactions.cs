using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Begins a transaction using the default isolation level (<see cref="IsolationLevel.ReadCommitted"/>).
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string? username = null,
        string? password = null)
        => BeginTransaction(serverOrInstance, database, integratedSecurity, IsolationLevel.ReadCommitted, username, password);

    /// <summary>
    /// Begins a transaction using the specified isolation level.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="isolationLevel">Isolation level to apply to the transaction.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        IsolationLevel isolationLevel,
        string? username = null,
        string? password = null)
    {
        lock (_syncRoot)
        {
            if (_transaction != null || _transactionInitializing)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);
            SqlConnection? connection = null;
            SqlTransaction? transaction = null;
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
    /// Begins a transaction asynchronously using the default isolation level.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual Task BeginTransactionAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
        => BeginTransactionAsync(serverOrInstance, database, integratedSecurity, IsolationLevel.ReadCommitted, cancellationToken, username, password);

    /// <summary>
    /// Begins a transaction asynchronously using the specified isolation level.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="isolationLevel">Isolation level to apply to the transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual async Task BeginTransactionAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        IsolationLevel isolationLevel,
        CancellationToken cancellationToken = default,
        string? username = null,
        string? password = null)
    {
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
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

            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

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
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual void Commit()
    {
        SqlTransaction? tx;
        SqlConnection? conn;
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
    /// <param name="cancellationToken">Token used to cancel the commit operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        SqlTransaction? tx;
        SqlConnection? conn;
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
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual void Rollback()
    {
        SqlTransaction? tx;
        SqlConnection? conn;
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
    /// <param name="cancellationToken">Token used to cancel the rollback operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        SqlTransaction? tx;
        SqlConnection? conn;
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
    protected virtual SqlTransaction BeginDbTransaction(SqlConnection connection, IsolationLevel isolationLevel) => connection.BeginTransaction(isolationLevel);

    /// <summary>
    /// Asynchronously begins a provider transaction on the supplied open connection.
    /// </summary>
    protected virtual async Task<SqlTransaction> BeginDbTransactionAsync(SqlConnection connection, IsolationLevel isolationLevel, CancellationToken cancellationToken)
    {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        return (SqlTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
        await Task.Yield();
        return connection.BeginTransaction(isolationLevel);
#endif
    }

    /// <summary>
    /// Commits the supplied provider transaction.
    /// </summary>
    protected virtual void CommitDbTransaction(SqlTransaction transaction) => transaction.Commit();

    /// <summary>
    /// Asynchronously commits the supplied provider transaction.
    /// </summary>
    protected virtual async Task CommitDbTransactionAsync(SqlTransaction transaction, CancellationToken cancellationToken)
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
    protected virtual void RollbackDbTransaction(SqlTransaction transaction) => transaction.Rollback();

    /// <summary>
    /// Asynchronously rolls back the supplied provider transaction.
    /// </summary>
    protected virtual async Task RollbackDbTransactionAsync(SqlTransaction transaction, CancellationToken cancellationToken)
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
    protected virtual void DisposeDbTransaction(SqlTransaction transaction) => transaction.Dispose();

    /// <summary>
    /// Disposes transaction resources created for the current operation.
    /// </summary>
    protected virtual void DisposeTransactionResources(SqlTransaction? transaction, SqlConnection? connection)
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
        ex is SqlException sqlEx &&
        sqlEx.Number is 4060 or 10928 or 10929 or 1205 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920;

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
