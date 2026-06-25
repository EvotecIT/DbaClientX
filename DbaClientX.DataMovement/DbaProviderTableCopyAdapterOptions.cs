namespace DBAClientX.DataMovement;

/// <summary>
/// Configures a provider-backed table-copy adapter.
/// </summary>
public sealed class DbaProviderTableCopyAdapterOptions
{
    /// <summary>Provider used by the adapter.</summary>
    public DbaTableCopyProvider Provider { get; set; }

    /// <summary>Connection string, or a SQLite database path for SQLite when no connection-string key/value syntax is used.</summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Fallback order columns used when a copy definition does not provide per-table order columns.</summary>
    public IReadOnlyList<string>? DefaultOrderByColumns { get; set; }

    /// <summary>Allows paged reads without explicit order columns. Use only for ad hoc copies.</summary>
    public bool AllowUnordered { get; set; }

    /// <summary>Optional SQL Server destination bulk-copy behavior.</summary>
    public SqlServerBulkInsertOptions? SqlServerOptions { get; set; }

    /// <summary>When true, missing source or destination tables are counted as empty instead of failing the copy.</summary>
    public bool TreatMissingTablesAsEmpty { get; set; }
}
