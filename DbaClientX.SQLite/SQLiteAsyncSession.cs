using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Represents an asynchronous SQLite connection session owned and managed by <see cref="SQLite"/>.
/// </summary>
/// <remarks>
/// Provider-specific connection and transaction objects remain internal to DBAClientX. Consumers
/// provide domain SQL, parameter values, and provider-neutral <see cref="IDataRecord"/> projections.
/// </remarks>
public sealed class SQLiteAsyncSession : IAsyncDisposable {
    private readonly SQLite _client;
    private readonly SqliteConnection _connection;
    private readonly SqliteTransaction? _transaction;
    private readonly bool _ownsConnection;
    private bool _disposed;

    internal SQLiteAsyncSession(SQLite client, SqliteConnection connection)
        : this(client, connection, transaction: null, ownsConnection: true) {
    }

    internal SQLiteAsyncSession(SQLite client, SqliteConnection connection, SqliteTransaction transaction)
        : this(client, connection, transaction, ownsConnection: false) {
    }

    private SQLiteAsyncSession(
        SQLite client,
        SqliteConnection connection,
        SqliteTransaction? transaction,
        bool ownsConnection) {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _transaction = transaction;
        _ownsConnection = ownsConnection;
    }

    /// <summary>Executes a statement that does not return rows.</summary>
    public Task<int> ExecuteNonQueryAsync(
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default) {
        ThrowIfDisposed();
        return _client.ExecuteSessionNonQueryAsync(_connection, _transaction, query, parameters, cancellationToken);
    }

    /// <summary>Executes a statement and returns the first column of the first row.</summary>
    public Task<object?> ExecuteScalarAsync(
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default) {
        ThrowIfDisposed();
        return _client.ExecuteSessionScalarAsync(_connection, _transaction, query, parameters, cancellationToken);
    }

    /// <summary>Executes a query and maps every row through a provider-neutral record.</summary>
    public Task<IReadOnlyList<T>> QueryAsListAsync<T>(
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null,
        Action<IDataRecord>? initialize = null,
        CancellationToken cancellationToken = default) {
        ThrowIfDisposed();
        return _client.ExecuteSessionQueryAsListAsync(
            _connection,
            _transaction,
            query,
            map,
            parameters,
            initialize,
            cancellationToken);
    }

    /// <summary>Runs related operations inside one SQLite transaction.</summary>
    public Task<TResult> RunInTransactionAsync<TResult>(
        Func<SQLiteAsyncSession, CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default) {
        ThrowIfDisposed();
        if (_transaction is not null) {
            throw new DbaTransactionException("A session transaction is already active.");
        }

        return _client.ExecuteSessionTransactionAsync(_connection, operation, cancellationToken);
    }

    /// <summary>Runs related operations inside one SQLite transaction.</summary>
    public async Task RunInTransactionAsync(
        Func<SQLiteAsyncSession, CancellationToken, Task> operation,
        CancellationToken cancellationToken = default) {
        if (operation is null) {
            throw new ArgumentNullException(nameof(operation));
        }

        await RunInTransactionAsync(async (session, token) => {
            await operation(session, token).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync() {
        if (_disposed) {
            return;
        }

        if (_ownsConnection) {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await _connection.DisposeAsync().ConfigureAwait(false);
#else
            _connection.Dispose();
            await Task.CompletedTask.ConfigureAwait(false);
#endif
        }

        _disposed = true;
    }

    private void ThrowIfDisposed() {
        if (_disposed) {
            throw new ObjectDisposedException(nameof(SQLiteAsyncSession));
        }
    }
}
