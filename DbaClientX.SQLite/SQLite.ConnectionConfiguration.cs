using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private volatile Action<SqliteConnection>? _configureConnection;
    private volatile SQLiteConnectionOptions? _connectionOptions;

    /// <summary>Gets or sets the operational profile for connections this client opens to execute SQL.</summary>
    /// <remarks>
    /// Null retains the existing command defaults. Assign a profile to use the same pragmas and pooling across
    /// commands, transactions, sessions, bulk operations and diagnostics. Profiles are copied on assignment and
    /// retrieval; modify a copy and assign it to change future connections. Configure the client before starting
    /// operations. Explicit method busy-timeout arguments take precedence over the profile. Read-only connections
    /// never apply journal, synchronous or checkpoint write pragmas. OpenDbConnection's explicit options override
    /// this profile and retain its established managed defaults when neither is supplied. Native database backup
    /// connections retain the dedicated backup policy and unpooled file lifetime.
    /// </remarks>
    public SQLiteConnectionOptions? ConnectionOptions
    {
        get => _connectionOptions?.Snapshot();
        set => _connectionOptions = value?.Snapshot();
    }

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

    private void ApplyConnectionConfiguration(SqliteConnection connection, bool applyProfile = true, bool retainTimeoutUpdates = false)
    {
        if (applyProfile && _connectionOptions is { } options)
            ApplyManagedConnectionOptions(connection, options, applyBusyTimeout: false);
        _configureConnection?.Invoke(connection);
        if (retainTimeoutUpdates) RetainConnectionTimeout(connection);
        else ConfigureConnectionTimeout(connection);
    }

    internal SqliteConnection CreateConfiguredConnection(string connectionString, SQLiteConnectionOptions? options = null)
    {
        options ??= _connectionOptions;
        if (options == null || !options.ReadOnly && !options.Pooling.HasValue)
            return new SqliteConnection(connectionString);

        var builder = new SqliteConnectionStringBuilder(connectionString);
        if (options?.ReadOnly == true)
        {
            if (builder.Mode == SqliteOpenMode.Memory || builder.DataSource == ":memory:")
                throw new ArgumentException("A read-only SQLite profile requires a file-backed database.", nameof(connectionString));
            builder.Mode = SqliteOpenMode.ReadOnly;
        }
        if (options?.Pooling is { } pooling) builder.Pooling = pooling;
        return new SqliteConnection(builder.ConnectionString);
    }

    internal async Task OpenConfiguredConnectionAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        await AwaitWithCallerCancellationAsync(() => OpenConnectionWithDiagnosticsAsync(connection, cancellationToken), cancellationToken).ConfigureAwait(false);
        await ApplyBusyTimeoutAsync(connection, ResolveConnectionBusyTimeout(connection.ConnectionString, null), cancellationToken).ConfigureAwait(false);
        ApplyConnectionConfiguration(connection);
    }
}
