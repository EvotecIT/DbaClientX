namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server server-level or database-level permission metadata.
/// </summary>
public sealed class SqlServerPermissionInfo
{
    /// <summary>Permission scope: Server or Database.</summary>
    public string Scope { get; set; } = string.Empty;

    /// <summary>Database name for database-level permissions.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Permission state code.</summary>
    public string State { get; set; } = string.Empty;

    /// <summary>Permission state description.</summary>
    public string StateDescription { get; set; } = string.Empty;

    /// <summary>Permission name.</summary>
    public string PermissionName { get; set; } = string.Empty;

    /// <summary>Securable class description.</summary>
    public string ClassDescription { get; set; } = string.Empty;

    /// <summary>Securable schema name where SQL Server exposes it.</summary>
    public string? SecurableSchema { get; set; }

    /// <summary>Securable name where SQL Server exposes it.</summary>
    public string? SecurableName { get; set; }

    /// <summary>Column name for column-level permissions when SQL Server exposes one.</summary>
    public string? SecurableColumn { get; set; }

    /// <summary>Grantee principal name.</summary>
    public string GranteeName { get; set; } = string.Empty;

    /// <summary>Grantor principal name.</summary>
    public string GrantorName { get; set; } = string.Empty;
}
