using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private readonly List<ConnectionTimeoutState> _connectionTimeouts = new();
    private int _timeoutRegistrations;

    private void ConfigureConnectionTimeout(SqliteConnection connection)
    {
        if (TryGetCommandTimeout(out int timeout)) connection.DefaultTimeout = timeout;
    }

    private void RetainConnectionTimeout(SqliteConnection connection)
    {
        lock (_syncRoot)
        {
            // Track only retained connections, and periodically prune without scanning on every new connection.
            if (++_timeoutRegistrations == 32)
            {
                _connectionTimeouts.RemoveAll(state => !state.Connection.TryGetTarget(out _));
                _timeoutRegistrations = 0;
            }
            _connectionTimeouts.Add(new ConnectionTimeoutState(connection));
            if (TryGetCommandTimeout(out int timeout)) connection.DefaultTimeout = timeout;
        }
    }

    /// <inheritdoc />
    protected override void OnCommandTimeoutChanged()
    {
        // Transaction startup already owns the client state lock. Use that same lock for
        // registration and notifications so reading timeout state cannot reverse lock order.
        lock (_syncRoot)
        {
            bool configured = TryGetCommandTimeout(out int timeout);
            for (int index = _connectionTimeouts.Count - 1; index >= 0; index--)
            {
                var state = _connectionTimeouts[index];
                if (state.Connection.TryGetTarget(out var connection))
                    connection.DefaultTimeout = configured ? timeout : state.OriginalTimeout;
                else
                    _connectionTimeouts.RemoveAt(index);
            }
        }
    }

    private sealed class ConnectionTimeoutState
    {
        public ConnectionTimeoutState(SqliteConnection connection)
        {
            Connection = new WeakReference<SqliteConnection>(connection);
            OriginalTimeout = connection.DefaultTimeout;
        }

        public WeakReference<SqliteConnection> Connection { get; }
        public int OriginalTimeout { get; }
    }
}
