namespace DBAClientX;

/// <summary>Configures provider-managed SQLite connections opened for domain storage workflows.</summary>
public sealed class SQLiteConnectionOptions
{
    /// <summary>Gets or sets whether the connection is opened read-only.</summary>
    public bool ReadOnly { get; set; }

    /// <summary>Gets or sets whether write connections use write-ahead logging.</summary>
    public bool EnableWriteAheadLogging { get; set; } = true;

    /// <summary>Gets or sets whether write connections use NORMAL synchronous durability.</summary>
    public bool UseNormalSynchronousMode { get; set; } = true;

    /// <summary>Gets or sets the optional busy timeout override in milliseconds.</summary>
    public int? BusyTimeoutMs { get; set; }

    /// <summary>Gets or sets the WAL auto-checkpoint page count. Zero leaves the provider default.</summary>
    public int WalAutoCheckpointPages { get; set; } = 1000;

    /// <summary>Gets or sets whether temporary storage uses memory.</summary>
    public bool UseMemoryTempStore { get; set; } = true;

    /// <summary>Gets or sets the SQLite cache size pragma. Negative values represent kibibytes.</summary>
    public int CacheSize { get; set; } = -2000;

    /// <summary>Gets or sets whether foreign-key enforcement is enabled.</summary>
    public bool EnableForeignKeys { get; set; } = true;
}
