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
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            _transactionConnection = new SqlConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction(isolationLevel);
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
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = (SqlTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
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
    /// Commits the currently active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
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
        }
        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            tx!.Commit();
#endif
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
        }
    }

    /// <summary>
    /// Rolls back the currently active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
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
        }
        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
            tx!.Rollback();
#endif
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
