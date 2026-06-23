namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server object dependency metadata from catalog views.
/// </summary>
public sealed class SqlServerDependencyInfo
{
    /// <summary>Dependency source, such as SqlExpression or ForeignKey.</summary>
    public string DependencyType { get; set; } = string.Empty;

    /// <summary>Schema that owns the referencing object.</summary>
    public string ReferencingSchema { get; set; } = string.Empty;

    /// <summary>Name of the referencing object.</summary>
    public string ReferencingName { get; set; } = string.Empty;

    /// <summary>SQL Server object type description for the referencing object.</summary>
    public string ReferencingType { get; set; } = string.Empty;

    /// <summary>Referenced server name when SQL Server exposes a cross-server dependency.</summary>
    public string? ReferencedServerName { get; set; }

    /// <summary>Referenced database name when SQL Server exposes a cross-database dependency.</summary>
    public string? ReferencedDatabaseName { get; set; }

    /// <summary>Referenced schema name when SQL Server exposes it.</summary>
    public string? ReferencedSchemaName { get; set; }

    /// <summary>Referenced entity name when SQL Server exposes it.</summary>
    public string? ReferencedEntityName { get; set; }

    /// <summary>Referenced class description, such as OBJECT_OR_COLUMN.</summary>
    public string? ReferencedClassDescription { get; set; }

    /// <summary>True when SQL Server reports caller-dependent resolution.</summary>
    public bool IsCallerDependent { get; set; }

    /// <summary>True when SQL Server reports ambiguous dependency resolution.</summary>
    public bool IsAmbiguous { get; set; }
}
