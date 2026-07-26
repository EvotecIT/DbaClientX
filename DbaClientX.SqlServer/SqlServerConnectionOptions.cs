using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>
/// Configures caller-owned SQL Server connection creation and authentication.
/// </summary>
public sealed class SqlServerConnectionOptions
{
    /// <summary>
    /// Optional reusable access-token callback. Reusing the same delegate preserves SqlClient connection pooling.
    /// </summary>
    public Func<SqlAuthenticationParameters, CancellationToken, Task<SqlAuthenticationToken>>? AccessTokenCallback { get; set; }

    /// <summary>
    /// Optional caller-owned connection factory. The returned connection is owned and disposed by DbaClientX.
    /// </summary>
    public Func<string, SqlConnection>? ConnectionFactory { get; set; }

    /// <summary>Compatibility profile applied to connections and bulk-copy operations.</summary>
    public SqlServerCompatibilityProfile CompatibilityProfile { get; set; }
}

/// <summary>Identifies SQL Server-compatible service behavior that needs explicit validation.</summary>
public enum SqlServerCompatibilityProfile
{
    /// <summary>Standard SQL Server behavior.</summary>
    Default,

    /// <summary>Microsoft Fabric Data Warehouse TDS endpoint behavior.</summary>
    FabricWarehouse
}
