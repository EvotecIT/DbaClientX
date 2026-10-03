using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    // Keep notifications independent of the transaction lock, which can span native database waits.
    // The base client releases its separate timeout-state lock before invoking the notification hook.
    private readonly object _connectionTimeoutSync = new();
    private readonly List<ConnectionTimeoutState> _connectionTimeouts = new();
    private int _timeoutRegistrations;

    private void ConfigureConnectionTimeout(SqliteConnection connection)
    {
        if (TryGetCommandTimeout(out int timeout)) connection.DefaultTimeout = timeout;
    }

    private void RetainConnectionTimeout(SqliteConnection connection)
    {
        lock (_connectionTimeoutSync)
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
        lock (_connectionTimeoutSync)
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
