namespace DBAClientX.PowerShell;

/// <summary>
/// Describes provider features exposed by DbaClientX.
/// </summary>
[Flags]
public enum DbaXProviderCapability
{
    /// <summary>No known provider capability.</summary>
    None = 0,

    /// <summary>Provider can execute text queries.</summary>
    Query = 1 << 0,

    /// <summary>Provider can execute non-query commands.</summary>
    NonQuery = 1 << 1,

    /// <summary>Provider can execute scalar commands.</summary>
    Scalar = 1 << 2,

    /// <summary>Provider can stream query rows.</summary>
    Streaming = 1 << 3,

    /// <summary>Provider can execute stored procedures.</summary>
    StoredProcedure = 1 << 4,

    /// <summary>Provider can perform provider-native bulk inserts.</summary>
    BulkInsert = 1 << 5,

    /// <summary>Provider can report table, column, and index metadata.</summary>
    Metadata = 1 << 6,

    /// <summary>Provider can participate in provider-backed table-copy operations.</summary>
    TableCopy = 1 << 7,

    /// <summary>Provider exposes transaction helpers.</summary>
    Transaction = 1 << 8,

    /// <summary>Provider exposes SQL Server-specific management APIs.</summary>
    SqlServerManagement = 1 << 9,

    /// <summary>Provider exposes SQL Server monitoring APIs.</summary>
    SqlServerMonitoring = 1 << 10,

    /// <summary>Provider exposes SQLite maintenance APIs.</summary>
    SQLiteMaintenance = 1 << 11,

    /// <summary>Provider exposes SQLite diagnostics APIs.</summary>
    SQLiteDiagnostics = 1 << 12
}
