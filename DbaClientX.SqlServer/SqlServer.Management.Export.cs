using DBAClientX.SqlServerManagement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Captures an immutable database script, dependency and permission review manifest.</summary>
    /// <param name="connectionString">Source connection, requiring database VIEW DEFINITION permission.</param>
    /// <param name="schema">Optional schema filter, compared using the source database collation.</param>
    /// <param name="name">Optional object name filter, compared using the source database collation.</param>
    /// <returns>A deterministic review plan with explicit unavailable objects and prerequisites.</returns>
    /// <remarks>Reuses the management script readers. Capture is not an atomic DDL snapshot; freeze
    /// source DDL when consistency is required. Permission metadata covers the database and is not
    /// restricted by the object filters. No scripts, grants, users, logins or files are created.
    /// Dynamic SQL, external references and omitted deployment prerequisites require review.
    /// SQL definitions may contain sensitive application literals.</remarks>
    public virtual SqlServerExportPlan GetSqlServerExportPlan(string connectionString,
        string? schema = null, string? name = null)
    {
        ValidateConnectionString(connectionString);
        var catalog = new List<ExportCatalogObject>();
        IReadOnlyList<SqlServerDependencyInfo> dependencies;
        string databaseName;
        var (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
        try
        {
            if (Convert.ToInt32(ExecuteScalar(connection, transaction,
                "SELECT COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION'), 0)")) != 1)
                throw new UnauthorizedAccessException("SQL Server export capture requires database VIEW DEFINITION permission.");
            databaseName = Convert.ToString(ExecuteScalar(connection, transaction, "SELECT DB_NAME()"))!;
            catalog.AddRange(ExecuteMappedQuery(connection, transaction, ExportCatalogQuery, record => new ExportCatalogObject
            {
                Schema = record.GetString(0), Name = record.GetString(1), Type = record.GetString(2),
                Code = record.GetString(3).Trim(), ParentSchema = record.GetString(4), ParentName = record.GetString(5),
                Selected = record.GetBoolean(6), PostCreate = record.GetBoolean(7)
            }, parameters: new Dictionary<string, object?> { ["@schema"] = schema, ["@name"] = name }));
            string query = ExpandSqlServerDependencyNames(SqlServerDependenciesManagementQuery.Replace(
                SqlServerDependenciesServerTriggerUnionToken, string.Empty), resolvedNames: true);
            dependencies = ExecuteMappedQuery(connection, transaction, query, SqlServerManagementMappers.MapDependency,
                parameters: new Dictionary<string, object?> { ["@schema"] = null, ["@name"] = null });
        }
        finally
        {
            if (dispose) DisposeConnection(connection);
        }

        // Pin the resolved catalog when the login's default database was used. Preserve all other connection options.
        string sourceConnection = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = databaseName }.ConnectionString;
        var selected = new HashSet<string>(catalog.Where(item => item.Selected).Select(item => item.Id), StringComparer.Ordinal);
        var scripts = GetSqlServerTableScripts(sourceConnection, schema, name)
            .Concat(GetSqlServerModuleScripts(sourceConnection, schema, name))
            .Where(script => selected.Contains(SqlServerExportPlanBuilder.Name(script.SchemaName, script.ObjectName))).ToArray();
        var represented = new HashSet<string>(scripts.Where(script => !string.IsNullOrWhiteSpace(script.Script))
            .Select(script => SqlServerExportPlanBuilder.Name(script.SchemaName, script.ObjectName)), StringComparer.Ordinal);
        var issues = new List<SqlServerExportIssue>();
        foreach (ExportCatalogObject item in catalog.Where(item => item.Selected))
        {
            if (item.Code == "C" || item.Code == "D" || item.Code == "F" || item.Code == "PK" || item.Code == "UQ") continue;
            if (!represented.Contains(item.Id))
                issues.Add(new SqlServerExportIssue(IsSqlModule(item.Code)
                    ? SqlServerExportIssueKind.UnavailableDefinition : SqlServerExportIssueKind.UnsupportedObject, item.Id, item.Type));
        }

        var byId = catalog.ToDictionary(item => item.Id, StringComparer.Ordinal);
        var resolved = new List<SqlServerDependencyInfo>(dependencies.Count);
        foreach (SqlServerDependencyInfo dependency in dependencies)
        {
            string id = SqlServerExportPlanBuilder.Name(dependency.ReferencingSchema, dependency.ReferencingName);
            byId.TryGetValue(id, out ExportCatalogObject? source);
            // DML trigger pseudo tables are supplied by SQL Server; an explicitly qualified real table is retained.
            if (source?.Code == "TR" && source.ParentName.Length != 0
                && string.IsNullOrEmpty(dependency.ReferencedServerName) && string.IsNullOrEmpty(dependency.ReferencedDatabaseName)
                && string.IsNullOrEmpty(dependency.ReferencedSchemaName) && dependency.ReferencedClassDescription == "OBJECT_OR_COLUMN"
                && (string.Equals(dependency.ReferencedEntityName, "inserted", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(dependency.ReferencedEntityName, "deleted", StringComparison.OrdinalIgnoreCase))) continue;
            // Inline defaults/checks are emitted with CREATE TABLE, so their expression dependencies belong to that table.
            if (source != null && (source.Code == "C" || source.Code == "D"))
            {
                dependency.ReferencingSchema = source.ParentSchema;
                dependency.ReferencingName = source.ParentName;
                if (source.PostCreate) dependency.DependencyType = "PostCreate";
            }
            resolved.Add(dependency);
        }
        foreach (ExportCatalogObject trigger in catalog.Where(item => item.Code == "TR" && item.ParentName.Length != 0))
            resolved.Add(new SqlServerDependencyInfo { DependencyType = "Parent", ReferencingSchema = trigger.Schema,
                ReferencingName = trigger.Name, ReferencedSchemaName = trigger.ParentSchema,
                ReferencedEntityName = trigger.ParentName, ReferencedClassDescription = "OBJECT_OR_COLUMN" });
        var permissions = GetSqlServerPermissions(sourceConnection).Where(permission => permission.Scope == "Database");
        return SqlServerExportPlanBuilder.Build(scripts, resolved, permissions, databaseName, issues);
    }

    private static bool IsSqlModule(string code)
        => code == "P" || code == "V" || code == "FN" || code == "IF" || code == "TF" || code == "TR";

    private sealed class ExportCatalogObject
    {
        internal string Schema { get; set; } = "";
        internal string Name { get; set; } = "";
        internal string Type { get; set; } = "";
        internal string Code { get; set; } = "";
        internal string ParentSchema { get; set; } = "";
        internal string ParentName { get; set; } = "";
        internal bool Selected { get; set; }
        internal bool PostCreate { get; set; }
        internal string Id => SqlServerExportPlanBuilder.Name(Schema, Name);
    }

    private const string ExportCatalogQuery = @"
SELECT s.name, o.name, o.type_desc, o.type,
       COALESCE(parent_schema.name, N''), COALESCE(parent.name, N''),
       CONVERT(bit, CASE WHEN (@schema IS NULL OR s.name = @schema)
         AND (@name IS NULL OR o.name = @name OR (o.type IN ('C','D','F','PK','UQ') AND parent.name = @name)) THEN 1 ELSE 0 END),
       CONVERT(bit, CASE WHEN checks.is_disabled = 1 OR checks.is_not_trusted = 1 THEN 1 ELSE 0 END)
FROM sys.objects AS o
INNER JOIN sys.schemas AS s ON s.schema_id = o.schema_id
LEFT JOIN sys.objects AS parent ON parent.object_id = o.parent_object_id
LEFT JOIN sys.schemas AS parent_schema ON parent_schema.schema_id = parent.schema_id
LEFT JOIN sys.check_constraints AS checks ON checks.object_id = o.object_id
WHERE o.is_ms_shipped = 0
UNION ALL
SELECT N'', t.name, t.type_desc, t.type, N'', N'', CONVERT(bit, CASE WHEN @schema IS NULL
    AND (@name IS NULL OR t.name = @name) THEN 1 ELSE 0 END), CONVERT(bit, 0)
FROM sys.triggers AS t WHERE t.parent_class = 0 AND t.is_ms_shipped = 0;";
}
