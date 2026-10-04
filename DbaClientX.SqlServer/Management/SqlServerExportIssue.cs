namespace DBAClientX.SqlServerManagement;

/// <summary>Reason why captured export metadata requires review.</summary>
public enum SqlServerExportIssueKind
{
    /// <summary>No usable definition was captured.</summary>
    UnavailableDefinition,
    /// <summary>The object or script family is outside the supported capture scope.</summary>
    UnsupportedObject,
    /// <summary>A named dependency or prerequisite is absent from the supplied scripts.</summary>
    MissingPrerequisite,
    /// <summary>Catalog resolution is ambiguous or caller-dependent.</summary>
    UnresolvedDependency,
    /// <summary>A reference names another server or explicitly qualifies a database.</summary>
    ExternalDependency,
    /// <summary>A cycle or a dependency on a cycle prevents ordering.</summary>
    CycleOrBlockedDependency
}

/// <summary>An immutable, structured review issue; it contains no script text.</summary>
public sealed class SqlServerExportIssue
{
    internal SqlServerExportIssue(SqlServerExportIssueKind kind, string objectName, string detail)
    { Kind = kind; ObjectName = objectName; Detail = detail; }
    /// <summary>Issue classification.</summary>
    public SqlServerExportIssueKind Kind { get; }
    /// <summary>Qualified source object or script identifier.</summary>
    public string ObjectName { get; }
    /// <summary>Missing prerequisite or resolution context.</summary>
    public string Detail { get; }
}
