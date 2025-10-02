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
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(database);

            _transactionConnection = new SqliteConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction();
        }
    }

    /// <summary>
    /// Starts a new transaction using the provider default isolation semantics.
    /// </summary>
    public virtual void BeginTransaction(string database, IsolationLevel isolationLevel)
        => BeginTransaction(database);

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
                if (_transaction != null || _transactionInitializing)
                {
                    throw new DbaTransactionException("Transaction already started.");
                }

                _transactionInitializing = true;
            }

            var connectionString = BuildConnectionString(database);

            connection = new SqliteConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            transaction = (SqliteTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
            transaction = connection.BeginTransaction();
#endif

            lock (_syncRoot)
            {
                if (_transaction != null)
                {
                    transaction.Dispose();
                    connection.Dispose();
                    _transactionInitializing = false;
                    throw new DbaTransactionException("Transaction already started.");
                }

                _transactionConnection = connection;
                _transaction = transaction;
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

            transaction?.Dispose();
            connection?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Asynchronously starts a new transaction using the provider default isolation semantics.
    /// </summary>
    public virtual Task BeginTransactionAsync(string database, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(database, cancellationToken);

    /// <summary>
    /// Commits the currently active transaction.
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
    /// Asynchronously commits the currently active transaction.
    /// </summary>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        SqliteTransaction? tx;
        SqliteConnection? conn;
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
        _transactionInitializing = false;
    }

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
}
