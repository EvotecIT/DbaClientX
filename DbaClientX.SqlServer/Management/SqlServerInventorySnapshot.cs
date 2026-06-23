using System.Collections.Generic;
using DBAClientX.Metadata;

namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server instance inventory snapshot assembled from reusable management readers.
/// </summary>
public sealed class SqlServerInventorySnapshot
{
    /// <summary>Selected SQL Server SERVERPROPERTY values.</summary>
    public IReadOnlyList<SqlServerInstancePropertyInfo> InstanceProperties { get; set; } = [];

    /// <summary>SQL Server instance configuration values.</summary>
    public IReadOnlyList<SqlServerConfigurationInfo> Configurations { get; set; } = [];

    /// <summary>Databases visible to the current connection.</summary>
    public IReadOnlyList<DbaDatabaseInfo> Databases { get; set; } = [];

    /// <summary>SQL Server Agent jobs visible to the current connection.</summary>
    public IReadOnlyList<SqlServerAgentJobInfo> AgentJobs { get; set; } = [];

    /// <summary>Server-level principals visible to the current connection.</summary>
    public IReadOnlyList<SqlServerPrincipalInfo> ServerPrincipals { get; set; } = [];
}
