using System;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private volatile Action<SqliteConnection>? _configureConnection;

    /// <summary>
    /// Gets or sets a callback that configures every connection this client opens to run SQL, after it is opened and its
    /// operational pragmas (busy timeout, journal settings) are applied, and before the caller's first command.
    /// </summary>
    /// <remarks>
    /// Use it to register connection-local functions and collations, for example
    /// <see cref="SQLiteUnicodeText.Register(SqliteConnection)"/> for Unicode case folding. It runs for the
    /// query, streaming, reader, non-query and scalar APIs (including read-only and transaction connections),
    /// sessions, <see cref="OpenDbConnection"/>, integrity checks, maintenance pragmas and diagnostics. Functions used
    /// by an index, view or trigger must be registered on every connection that touches it, including other tools'.
    /// An exception from the callback fails the operation and the connection is closed; depending on the API it reaches
    /// the caller as is or wrapped in <see cref="DbaQueryExecutionException"/>. The callback can run concurrently (for
    /// example from <see cref="RunQueriesInParallel"/>), so it must be thread-safe. Registrations are removed when a
    /// connection is closed, also when the provider pools it.
    /// </remarks>
    public Action<SqliteConnection>? ConfigureConnection
    {
        get => _configureConnection;
        set => _configureConnection = value;
    }

    private void ApplyConnectionConfiguration(SqliteConnection connection)
        => _configureConnection?.Invoke(connection);
}
