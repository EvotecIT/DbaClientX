namespace DBAClientX.PowerShell;

/// <summary>
/// Selects the SQL Server-specific management metadata shape returned by <c>Get-DbaXSqlServerManagement</c>.
/// </summary>
public enum DbaXSqlServerManagementType
{
    /// <summary>SQL Server Agent job definitions.</summary>
    AgentJob,

    /// <summary>SQL Server Agent job step definitions.</summary>
    AgentJobStep,

    /// <summary>SQL Server Agent schedules.</summary>
    AgentSchedule,

    /// <summary>Server-level principals.</summary>
    ServerPrincipal,

    /// <summary>Database-level principals for the current database.</summary>
    DatabasePrincipal,

    /// <summary>Server-level and current database role memberships.</summary>
    RoleMembership,

    /// <summary>Server-level and current database permissions.</summary>
    Permission,

    /// <summary>Selected SQL Server instance properties.</summary>
    InstanceProperty,

    /// <summary>SQL Server instance configuration values.</summary>
    Configuration
}
