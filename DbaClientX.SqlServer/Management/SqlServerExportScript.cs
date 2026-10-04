namespace DBAClientX.SqlServerManagement;

/// <summary>Immutable captured script with exact-text integrity and ordering metadata.</summary>
public sealed class SqlServerExportScript
{
    internal SqlServerExportScript(SqlServerScriptInfo source, string id, IEnumerable<string> prerequisites)
    {
        Id = id; ScriptType = source.ScriptType; SchemaName = source.SchemaName;
        ObjectName = source.ObjectName; ObjectType = source.ObjectType; Script = source.Script;
        RequiredScriptIds = Array.AsReadOnly(prerequisites.OrderBy(value => value, StringComparer.Ordinal).ToArray());
        ContentFingerprint = SqlServerExportPlanBuilder.FingerprintScript(Script);
    }
    /// <summary>Stable script identifier; preserves identifier case and escaped names.</summary>
    public string Id { get; }
    /// <summary>Script family, such as Table, Module or TablePostCreate.</summary>
    public string ScriptType { get; }
    /// <summary>Owning schema.</summary>
    public string SchemaName { get; }
    /// <summary>Object name.</summary>
    public string ObjectName { get; }
    /// <summary>Native object type description.</summary>
    public string ObjectType { get; }
    /// <summary>Captured SQL text. It may contain sensitive application literals; review before sharing.</summary>
    public string Script { get; }
    /// <summary>SHA-256 of exact UTF-8 script bytes without a byte-order mark.</summary>
    public string ContentFingerprint { get; }
    /// <summary>Resolved local scripts that precede this script.</summary>
    public IReadOnlyList<string> RequiredScriptIds { get; }
}
