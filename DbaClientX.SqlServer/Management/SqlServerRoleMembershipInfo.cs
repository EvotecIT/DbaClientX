namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server server-level or database-level role membership metadata.
/// </summary>
public sealed class SqlServerRoleMembershipInfo
{
    /// <summary>Membership scope: Server or Database.</summary>
    public string Scope { get; set; } = string.Empty;

    /// <summary>Database name for database-level role memberships.</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Role name.</summary>
    public string RoleName { get; set; } = string.Empty;

    /// <summary>Role type description.</summary>
    public string? RoleTypeDescription { get; set; }

    /// <summary>Member principal name.</summary>
    public string MemberName { get; set; } = string.Empty;

    /// <summary>Member type description.</summary>
    public string? MemberTypeDescription { get; set; }
}
