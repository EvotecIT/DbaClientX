using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Represents a SQLite connection session owned and managed by <see cref="SQLite"/>.
/// </summary>
/// <remarks>
/// The session keeps provider-specific connection and transaction objects inside DBAClientX while
/// allowing callers to run related commands against the same SQLite connection.
/// </remarks>
public sealed class SQLiteSession : IDisposable
{
    private readonly SQLite _client;
    private readonly SqliteConnection _connection;
    private readonly SqliteTransaction? _transaction;
    private readonly bool _ownsConnection;
    private bool _disposed;

    internal SQLiteSession(SQLite client, SqliteConnection connection)
        : this(client, connection, transaction: null, ownsConnection: true)
    {
    }

    internal SQLiteSession(SQLite client, SqliteConnection connection, SqliteTransaction transaction)
        : this(client, connection, transaction, ownsConnection: false)
    {
    }

    private SQLiteSession(
        SQLite client,
        SqliteConnection connection,
        SqliteTransaction? transaction,
        bool ownsConnection)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _transaction = transaction;
        _ownsConnection = ownsConnection;
    }

    /// <summary>
    /// Executes a SQL statement that does not return rows using this session's connection.
    /// </summary>
    /// <param name="query">SQL command text.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <returns>The number of rows affected by the command.</returns>
    public int ExecuteNonQuery(string query, IDictionary<string, object?>? parameters = null)
    {
        ThrowIfDisposed();
        return _client.ExecuteSessionNonQuery(_connection, _transaction, query, parameters);
    }

    /// <summary>
    /// Executes a SQL statement and returns the first column of the first row.
    /// </summary>
    /// <param name="query">SQL command text.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <returns>The scalar value returned by the provider.</returns>
    public object? ExecuteScalar(string query, IDictionary<string, object?>? parameters = null)
    {
        ThrowIfDisposed();
        return _client.ExecuteSessionScalar(_connection, _transaction, query, parameters);
    }

    /// <summary>
    /// Executes a query and maps each row through a provider-neutral <see cref="IDataRecord"/>.
    /// </summary>
    /// <typeparam name="T">The row projection type.</typeparam>
    /// <param name="query">SQL query text.</param>
    /// <param name="map">Mapping callback invoked for each row.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="initialize">Optional callback invoked once after the reader opens.</param>
    /// <returns>The mapped rows.</returns>
    public IReadOnlyList<T> QueryAsList<T>(
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        Action<IDataRecord>? initialize = null)
    {
        ThrowIfDisposed();
        return _client.ExecuteSessionQueryAsList(_connection, _transaction, query, map, parameters, initialize);
    }

    /// <summary>
    /// Runs a group of operations inside one SQLite transaction using this session's connection.
    /// </summary>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="operation">Callback that receives a transaction-bound session.</param>
    /// <returns>The callback result after the transaction commits.</returns>
    public TResult RunInTransaction<TResult>(Func<SQLiteSession, TResult> operation)
    {
        ThrowIfDisposed();
        if (_transaction != null)
        {
            throw new DbaTransactionException("A session transaction is already active.");
        }

        return _client.ExecuteSessionTransaction(_connection, operation);
    }

    /// <summary>
    /// Runs a group of operations inside one SQLite transaction using this session's connection.
    /// </summary>
    /// <param name="operation">Callback that receives a transaction-bound session.</param>
    public void RunInTransaction(Action<SQLiteSession> operation)
    {
        if (operation == null)
        {
            throw new ArgumentNullException(nameof(operation));
        }

        RunInTransaction(session =>
        {
            operation(session);
            return true;
        });
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        if (_ownsConnection)
        {
            _connection.Dispose();
        }

        _disposed = true;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SQLiteSession));
        }
    }
}
