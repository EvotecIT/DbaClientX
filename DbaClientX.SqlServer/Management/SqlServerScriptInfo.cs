namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server script text for an object discovered through catalog metadata.
/// </summary>
public sealed class SqlServerScriptInfo
{
    /// <summary>Script family, such as Module or Table.</summary>
    public string ScriptType { get; set; } = string.Empty;

    /// <summary>Schema that owns the scripted object.</summary>
    public string SchemaName { get; set; } = string.Empty;

    /// <summary>Name of the scripted object.</summary>
    public string ObjectName { get; set; } = string.Empty;

    /// <summary>SQL Server object type description.</summary>
    public string ObjectType { get; set; } = string.Empty;

    /// <summary>Generated or catalog-provided T-SQL script.</summary>
    public string Script { get; set; } = string.Empty;
}
