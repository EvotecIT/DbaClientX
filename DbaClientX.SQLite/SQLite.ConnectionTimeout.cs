using System;
using System.Collections.Generic;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private readonly object _connectionTimeoutSync = new();
    private readonly List<ConnectionTimeoutState> _connectionTimeouts = new();
    private int _timeoutRegistrations;

    private SqliteConnection ConfigureConnectionTimeout(SqliteConnection connection)
    {
        if (TryGetCommandTimeout(out int timeout)) connection.DefaultTimeout = timeout;
        return connection;
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
            connection.DefaultTimeout = TryGetCommandTimeout(out int timeout)
                ? timeout : new SqliteConnectionStringBuilder(connection.ConnectionString).DefaultTimeout;
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
            OriginalTimeout = new SqliteConnectionStringBuilder(connection.ConnectionString).DefaultTimeout;
        }

        public WeakReference<SqliteConnection> Connection { get; }
        public int OriginalTimeout { get; }
    }
}
