namespace DBAClientX.SqlServerManagement;

/// <summary>Immutable permission metadata for review; it is not an executable permission statement.</summary>
public sealed class SqlServerExportPermission
{
    internal SqlServerExportPermission(SqlServerPermissionInfo source)
    {
        Scope = source.Scope; DatabaseName = source.DatabaseName; State = source.State;
        StateDescription = source.StateDescription; PermissionName = source.PermissionName;
        ClassDescription = source.ClassDescription; SecurableSchema = source.SecurableSchema;
        SecurableName = source.SecurableName; SecurableColumn = source.SecurableColumn;
        GranteeName = source.GranteeName; GrantorName = source.GrantorName;
    }
    /// <summary>Permission scope.</summary>
    public string Scope { get; }
    /// <summary>Source database when present.</summary>
    public string? DatabaseName { get; }
    /// <summary>Native permission state code.</summary>
    public string State { get; }
    /// <summary>Native permission state description.</summary>
    public string StateDescription { get; }
    /// <summary>Permission name.</summary>
    public string PermissionName { get; }
    /// <summary>Securable class.</summary>
    public string ClassDescription { get; }
    /// <summary>Securable schema when present.</summary>
    public string? SecurableSchema { get; }
    /// <summary>Securable name when present.</summary>
    public string? SecurableName { get; }
    /// <summary>Securable column when present.</summary>
    public string? SecurableColumn { get; }
    /// <summary>Grantee name.</summary>
    public string GranteeName { get; }
    /// <summary>Grantor name.</summary>
    public string GrantorName { get; }
    internal IEnumerable<string?> Fields => new[] { Scope, DatabaseName, State, StateDescription,
        PermissionName, ClassDescription, SecurableSchema, SecurableName, SecurableColumn, GranteeName, GrantorName };
}
