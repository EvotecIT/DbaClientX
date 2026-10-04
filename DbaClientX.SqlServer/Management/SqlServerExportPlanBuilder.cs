using System.Security.Cryptography;
using System.Text;

namespace DBAClientX.SqlServerManagement;

internal static class SqlServerExportPlanBuilder
{
    private sealed class Node
    {
        internal Node(SqlServerScriptInfo source) { Source = source; Id = source.ScriptType + ":" + Name(source.SchemaName, source.ObjectName); }
        internal SqlServerScriptInfo Source { get; }
        internal string Id { get; }
        internal HashSet<string> Required { get; } = new(StringComparer.Ordinal);
        internal HashSet<string> Dependents { get; } = new(StringComparer.Ordinal);
    }

    internal static SqlServerExportPlan Build(IEnumerable<SqlServerScriptInfo> scripts,
        IEnumerable<SqlServerDependencyInfo> dependencies, IEnumerable<SqlServerPermissionInfo>? permissions,
        string? databaseName, IEnumerable<SqlServerExportIssue> extraIssues)
    {
        if (scripts is null) throw new ArgumentNullException(nameof(scripts));
        if (dependencies is null) throw new ArgumentNullException(nameof(dependencies));
        var issues = extraIssues.ToList();
        var nodes = new Dictionary<string, Node>(StringComparer.Ordinal);
        var primary = new Dictionary<string, Node>(StringComparer.Ordinal);
        foreach (SqlServerScriptInfo input in scripts)
        {
            if (input is null || string.IsNullOrWhiteSpace(input.ObjectName))
                throw new ArgumentException("Scripts require a nonempty object name.", nameof(scripts));
            var source = new SqlServerScriptInfo { ScriptType = input.ScriptType, SchemaName = input.SchemaName ?? "",
                ObjectName = input.ObjectName, ObjectType = input.ObjectType ?? "", Script = input.Script ?? "" };
            string name = Name(source.SchemaName, source.ObjectName);
            if (source.ScriptType != "Table" && source.ScriptType != "Module" && source.ScriptType != "TablePostCreate"
                || source.SchemaName.Length == 0)
            { issues.Add(new(SqlServerExportIssueKind.UnsupportedObject, name, source.ObjectType + "/" + source.ScriptType)); continue; }
            if (string.IsNullOrWhiteSpace(source.Script))
            { issues.Add(new(SqlServerExportIssueKind.UnavailableDefinition, name, source.ObjectType)); continue; }
            var node = new Node(source);
            if (nodes.ContainsKey(node.Id)) throw new ArgumentException("Duplicate script identity: " + node.Id, nameof(scripts));
            nodes.Add(node.Id, node);
            if (source.ScriptType != "TablePostCreate")
            {
                if (primary.ContainsKey(name)) throw new ArgumentException("Conflicting primary object identity: " + name, nameof(scripts));
                primary.Add(name, node);
            }
            if (source.SchemaName != "dbo") issues.Add(new(SqlServerExportIssueKind.MissingPrerequisite, name, "Schema " + source.SchemaName));
            if (source.ObjectType.StartsWith("CLR_", StringComparison.Ordinal) || source.ObjectType == "AGGREGATE_FUNCTION")
                issues.Add(new(SqlServerExportIssueKind.MissingPrerequisite, name, "CLR assembly and deployment policy"));
        }
        foreach (Node post in nodes.Values.Where(node => node.Source.ScriptType == "TablePostCreate"))
        {
            string name = Name(post.Source.SchemaName, post.Source.ObjectName);
            if (primary.TryGetValue(name, out Node? table) && table.Source.ScriptType == "Table") AddEdge(post, table);
            else issues.Add(new(SqlServerExportIssueKind.MissingPrerequisite, post.Id, "Table " + name));
        }
        foreach (SqlServerDependencyInfo dependency in dependencies)
        {
            if (dependency is null) throw new ArgumentException("Null dependency.", nameof(dependencies));
            string name = Name(dependency.ReferencingSchema ?? "", dependency.ReferencingName ?? "");
            if (!primary.TryGetValue(name, out Node? node)) continue;
            if ((dependency.DependencyType == "ForeignKey" || dependency.DependencyType == "PostCreate")
                && nodes.TryGetValue("TablePostCreate:" + name, out Node? post)) node = post;
            string target = Name(dependency.ReferencedSchemaName ?? "", dependency.ReferencedEntityName ?? "");
            if (dependency.IsCallerDependent || dependency.IsAmbiguous || string.IsNullOrEmpty(dependency.ReferencedEntityName))
                issues.Add(new(SqlServerExportIssueKind.UnresolvedDependency, node.Id, target));
            else if (!string.IsNullOrEmpty(dependency.ReferencedServerName) || !string.IsNullOrEmpty(dependency.ReferencedDatabaseName))
                issues.Add(new(SqlServerExportIssueKind.ExternalDependency, node.Id,
                    (dependency.ReferencedServerName ?? "") + "/" + (dependency.ReferencedDatabaseName ?? "") + "/" + target));
            else if (dependency.ReferencedClassDescription != "OBJECT_OR_COLUMN"
                || !primary.TryGetValue(target, out Node? required))
                issues.Add(new(SqlServerExportIssueKind.MissingPrerequisite, node.Id,
                    (dependency.ReferencedClassDescription ?? "Unknown class") + " " + target));
            else if (node.Id != required.Id) AddEdge(node, required);
        }
        var count = nodes.ToDictionary(pair => pair.Key, pair => pair.Value.Required.Count, StringComparer.Ordinal);
        var ready = new SortedSet<string>(Comparer<string>.Create((left, right) => {
            int phase = (nodes[left].Source.ScriptType == "TablePostCreate" ? 1 : 0)
                .CompareTo(nodes[right].Source.ScriptType == "TablePostCreate" ? 1 : 0);
            return phase != 0 ? phase : StringComparer.Ordinal.Compare(left, right);
        }));
        foreach (var pair in count.Where(pair => pair.Value == 0)) ready.Add(pair.Key);
        var ordered = new List<string>();
        while (ready.Count != 0)
        {
            string id = ready.Min!; ready.Remove(id); ordered.Add(id);
            foreach (string dependent in nodes[id].Dependents)
                if (--count[dependent] == 0) ready.Add(dependent);
        }
        foreach (string id in count.Where(pair => pair.Value != 0).Select(pair => pair.Key))
            issues.Add(new(SqlServerExportIssueKind.CycleOrBlockedDependency, id,
                "Unordered prerequisites: " + string.Join(", ", nodes[id].Required.Where(required => count[required] != 0).OrderBy(value => value, StringComparer.Ordinal))));
        var immutable = nodes.Values.OrderBy(node => node.Id, StringComparer.Ordinal)
            .Select(node => new SqlServerExportScript(node.Source, node.Id, node.Required)).ToArray();
        var byId = immutable.ToDictionary(script => script.Id, StringComparer.Ordinal);
        var copiedPermissions = (permissions ?? Array.Empty<SqlServerPermissionInfo>()).Select(permission =>
            new SqlServerExportPermission(permission ?? throw new ArgumentException("Null permission.", nameof(permissions))))
            .OrderBy(permission => permission.Scope, StringComparer.Ordinal)
            .ThenBy(permission => permission.ClassDescription, StringComparer.Ordinal)
            .ThenBy(permission => permission.SecurableSchema, StringComparer.Ordinal)
            .ThenBy(permission => permission.SecurableName, StringComparer.Ordinal)
            .ThenBy(permission => permission.SecurableColumn, StringComparer.Ordinal)
            .ThenBy(permission => permission.GranteeName, StringComparer.Ordinal)
            .ThenBy(permission => permission.PermissionName, StringComparer.Ordinal)
            .ThenBy(permission => permission.State, StringComparer.Ordinal)
            .ThenBy(permission => Fingerprint(permission.Fields), StringComparer.Ordinal).ToArray();
        var stableIssues = issues.GroupBy(issue => Fingerprint(new[] { issue.Kind.ToString(), issue.ObjectName, issue.Detail }), StringComparer.Ordinal)
            .Select(group => group.First()).OrderBy(issue => issue.Kind).ThenBy(issue => issue.ObjectName, StringComparer.Ordinal)
            .ThenBy(issue => issue.Detail, StringComparer.Ordinal).ToArray();
        return new SqlServerExportPlan(databaseName, Array.AsReadOnly(immutable),
            Array.AsReadOnly(ordered.Select(id => byId[id]).ToArray()), Array.AsReadOnly(stableIssues), Array.AsReadOnly(copiedPermissions));
    }

    internal static string Name(string schema, string name)
        => SqlServerManagementScripting.QuoteName(schema) + "." + SqlServerManagementScripting.QuoteName(name);
    private static void AddEdge(Node node, Node required)
    { if (node.Required.Add(required.Id)) required.Dependents.Add(node.Id); }

    internal static string Fingerprint(IEnumerable<string?> fields, bool frameFields = true)
    {
        using var hash = SHA256.Create();
        using var stream = new CryptoStream(Stream.Null, hash, CryptoStreamMode.Write);
        var encoding = new UTF8Encoding(false, true);
        if (frameFields)
        {
            using var writer = new BinaryWriter(stream, encoding, leaveOpen: true);
            foreach (string? field in fields) { writer.Write(field != null); if (field != null) writer.Write(field); }
            writer.Flush();
        }
        else
        {
            using var writer = new StreamWriter(stream, encoding, 4096, leaveOpen: true);
            foreach (string? field in fields) if (field != null) writer.Write(field);
            writer.Flush();
        }
        stream.FlushFinalBlock();
        return BitConverter.ToString(hash.Hash!).Replace("-", "").ToLowerInvariant();
    }
}
