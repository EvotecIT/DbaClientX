namespace DBAClientX.SqlServerManagement;

/// <summary>An immutable, dependency-ordered SQL Server export review manifest.</summary>
/// <remarks>This does not execute scripts, write files, or establish migration or recovery readiness.
/// Fingerprints cover exact supplied script text and metadata; SQL literal line endings are not normalized.</remarks>
public sealed class SqlServerExportPlan
{
    internal SqlServerExportPlan(string? databaseName, IReadOnlyList<SqlServerExportScript> scripts,
        IReadOnlyList<SqlServerExportScript> ordered, IReadOnlyList<SqlServerExportIssue> issues,
        IReadOnlyList<SqlServerExportPermission> permissions)
    {
        SourceDatabaseName = databaseName; Scripts = scripts; OrderedScripts = ordered;
        Issues = issues; Permissions = permissions;
        Fingerprint = SqlServerExportPlanBuilder.Fingerprint(FingerprintFields());
    }
    /// <summary>Manifest contract version.</summary>
    public int FormatVersion => 1;
    /// <summary>Source database name when supplied or captured.</summary>
    public string? SourceDatabaseName { get; }
    /// <summary>All captured scripts, in stable identifier order, including scripts needing review.</summary>
    public IReadOnlyList<SqlServerExportScript> Scripts { get; }
    /// <summary>Scripts ordered by resolved local dependencies; cycle-blocked scripts are excluded.</summary>
    public IReadOnlyList<SqlServerExportScript> OrderedScripts { get; }
    /// <summary>Explicit unavailable definitions, prerequisites and ordering issues.</summary>
    public IReadOnlyList<SqlServerExportIssue> Issues { get; }
    /// <summary>Copied permission metadata; no grants, users, roles or logins are created.</summary>
    public IReadOnlyList<SqlServerExportPermission> Permissions { get; }
    /// <summary>Whether the manifest has explicit review issues; false does not imply deployment readiness.</summary>
    public bool HasIssues => Issues.Count != 0;
    /// <summary>Deterministic SHA-256 of captured content, order, permissions and issues.</summary>
    public string Fingerprint { get; }
    /// <summary>Limits that apply to every plan, independent of discovered object-specific issues.</summary>
    public IReadOnlyList<string> Limitations { get; } = Array.AsReadOnly(new[] {
        "Capture is not an atomic DDL snapshot; freeze source DDL when consistency is required.",
        "Dynamic SQL and runtime resolution can have dependencies absent from native catalog metadata.",
        "Schema owners, user types, assemblies, principals, permissions and instance configuration are not scripted.",
        "Table data, backup media, credentials, password hashes and private keys are not captured.",
        "Permission metadata is for review and does not establish effective access on a destination.",
        "SQL scripts can contain sensitive application literals; review them before storage or sharing." });

    private IEnumerable<string?> FingerprintFields()
    {
        yield return "DbaClientX.SqlServer.Export.1"; yield return SourceDatabaseName;
        yield return Scripts.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
        foreach (var script in Scripts)
        {
            yield return script.Id; yield return script.ObjectType; yield return script.ContentFingerprint;
            yield return script.RequiredScriptIds.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
            foreach (string id in script.RequiredScriptIds) yield return id;
        }
        yield return OrderedScripts.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
        foreach (var script in OrderedScripts) yield return script.Id;
        yield return Issues.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
        foreach (var issue in Issues)
        { yield return issue.Kind.ToString(); yield return issue.ObjectName; yield return issue.Detail; }
        yield return Permissions.Count.ToString(System.Globalization.CultureInfo.InvariantCulture);
        foreach (var permission in Permissions)
            foreach (string? field in permission.Fields) yield return field;
    }

    /// <summary>Builds a review manifest from existing script/dependency/permission reader results without I/O.</summary>
    /// <param name="scripts">One script per family and exact schema/name identity.</param>
    /// <param name="dependencies">Native or explicitly supplied dependency metadata.</param>
    /// <param name="permissions">Optional permission metadata to snapshot.</param>
    /// <param name="sourceDatabaseName">Optional source context; explicitly database-qualified references remain review prerequisites.</param>
    /// <returns>An immutable content snapshot with deterministic order and issue reporting.</returns>
    /// <remarks>Identifier matching is ordinal and case-sensitive; supply resolved catalog identities.
    /// Callers own completeness of offline inputs. Self references do not add ordering edges.</remarks>
    public static SqlServerExportPlan Create(IEnumerable<SqlServerScriptInfo> scripts,
        IEnumerable<SqlServerDependencyInfo> dependencies, IEnumerable<SqlServerPermissionInfo>? permissions = null,
        string? sourceDatabaseName = null)
        => SqlServerExportPlanBuilder.Build(scripts, dependencies, permissions, sourceDatabaseName, Array.Empty<SqlServerExportIssue>());
}
