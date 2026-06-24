using System;

namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server server-level or database-level principal metadata.
/// </summary>
public sealed class SqlServerPrincipalInfo
{
    /// <summary>Principal scope: Server or Database.</summary>
    public string Scope { get; set; } = string.Empty;

    /// <summary>Database name for database-level principals.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Principal name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>SQL Server principal type code.</summary>
    public string Type { get; set; } = string.Empty;

    /// <summary>SQL Server principal type description.</summary>
    public string TypeDescription { get; set; } = string.Empty;

    /// <summary>Principal SID as a hexadecimal string.</summary>
    public string? Sid { get; set; }

    /// <summary>Default database for server principals when available.</summary>
    public string? DefaultDatabaseName { get; set; }

    /// <summary>Default schema for database principals when available.</summary>
    public string? DefaultSchemaName { get; set; }

    /// <summary>Authentication type for database principals when available.</summary>
    public string? AuthenticationType { get; set; }

    /// <summary>True when the principal is disabled, when SQL Server exposes that state.</summary>
    public bool? IsDisabled { get; set; }

    /// <summary>True when the principal is a fixed role, when SQL Server exposes that state.</summary>
    public bool? IsFixedRole { get; set; }

    /// <summary>Date the principal was created.</summary>
    public DateTime? Created { get; set; }

    /// <summary>Date the principal was last modified.</summary>
    public DateTime? Modified { get; set; }
}
