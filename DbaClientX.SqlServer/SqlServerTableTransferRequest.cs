using System.Data;

namespace DBAClientX;

/// <summary>Describes a single SQL Server table-to-table streaming bulk transfer.</summary>
/// <remarks>
/// This workflow appends rows without checkpoints or content verification. Use the provider table-copy runner
/// when a copy requires resumability, transformations or content verification.
/// </remarks>
public sealed class SqlServerTableTransferRequest
{
    /// <summary>Source database connection string. Capture uses an owned, non-enlisted, non-pooled connection.</summary>
    public string SourceConnectionString { get; set; } = string.Empty;

    /// <summary>Destination database connection string. Writes use an owned, non-enlisted, non-pooled connection.</summary>
    public string DestinationConnectionString { get; set; } = string.Empty;

    /// <summary>Source table name, optionally schema-qualified. An omitted schema means dbo; database qualifiers are not accepted.</summary>
    public string SourceTable { get; set; } = string.Empty;

    /// <summary>Destination table name, optionally schema-qualified. An omitted schema means dbo; database qualifiers are not accepted.</summary>
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Optional source projection. Each entry is one literal column name; omit generated destination columns as needed.</summary>
    public IReadOnlyList<string>? SourceColumns { get; set; }

    /// <summary>Native mapping, identity, constraint, progress and optional staging-table creation behavior. When omitted, CheckConstraints is enabled.</summary>
    public SqlServerBulkInsertOptions? BulkOptions { get; set; }

    /// <summary>Rows per native bulk batch. The default is 5,000.</summary>
    public int BatchSize { get; set; } = 5000;

    /// <summary>Timeout in seconds for source and metadata commands. Zero disables the timeout.</summary>
    public int CommandTimeout { get; set; } = 600;

    /// <summary>Timeout in seconds for native bulk copy. Zero disables the timeout.</summary>
    public int BulkCopyTimeout { get; set; } = 600;

    /// <summary>Owned source transaction isolation: ReadCommitted, Snapshot or Serializable. Snapshot must already be enabled on the database.</summary>
    public IsolationLevel SourceIsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>Commits each native batch separately. Earlier batches and optional created schema/table can remain after failure or cancellation.</summary>
    /// <remarks>Leave false to commit destination writes in one transaction after the source reader and transaction have completed.</remarks>
    public bool CommitEachBatch { get; set; }

    /// <summary>Optional source authentication and connection factory. A factory must return a closed connection with the supplied connection string.</summary>
    public SqlServerConnectionOptions? SourceConnectionOptions { get; set; }

    /// <summary>Optional destination authentication and connection factory. A factory must return a closed connection with the supplied connection string.</summary>
    public SqlServerConnectionOptions? DestinationConnectionOptions { get; set; }
}
