using Microsoft.Data.Sqlite;

namespace DBAClientX;

public sealed partial class SQLiteTableCopyAdapter
{
    private volatile SQLiteConnectionOptions? _connectionOptions;
    private volatile Action<SqliteConnection>? _configureConnection;

    /// <summary>Gets or sets the shared SQLite operational profile for adapter connections.</summary>
    /// <remarks>
    /// Profiles are copied on assignment and retrieval. Configure the adapter before starting a copy.
    /// Null retains the existing provider defaults. The profile applies to reads, writes, schema preflight,
    /// verification and atomic checkpoints; read-only connections skip write pragmas.
    /// </remarks>
    public SQLiteConnectionOptions? ConnectionOptions
    {
        get => _connectionOptions?.Snapshot();
        set => _connectionOptions = value?.Snapshot();
    }

    /// <summary>Gets or sets registrations applied before the first SQL command on every adapter connection.</summary>
    /// <remarks>
    /// Register functions and collations used by views, indexes and triggers here, for example
    /// <see cref="SQLiteUnicodeText.Register(SqliteConnection)"/>. The callback must be thread-safe.
    /// A callback failure aborts the operation and the connection is disposed.
    /// </remarks>
    public Action<SqliteConnection>? ConfigureConnection
    {
        get => _configureConnection;
        set => _configureConnection = value;
    }

    private SQLite CreateClient() => new()
    {
        CommandTimeout = CommandTimeout,
        ConnectionOptions = _connectionOptions,
        ConfigureConnection = _configureConnection
    };

    private SqliteConnection CreateConnection()
    {
        using var client = CreateClient();
        return client.CreateConfiguredConnection(ResolveSQLiteConnectionString());
    }

    private async Task OpenConnectionAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        using var client = CreateClient();
        await client.OpenConfiguredConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
    }
}
