using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Reflection;
using DBAClientX;
using DBAClientX.SqlServerManagement;

namespace DbaClientX.Tests;

public sealed class SqlServerExportPlanTests
{
    [Theory]
    [InlineData("TR", "SQL_TRIGGER")]
    [InlineData("TA", "CLR_TRIGGER")]
    public void NativeTriggerParentProjectionOrdersBothKindsAndReportsFilteredParents(string code, string type)
    {
        // CLR activation is not required: exercise the native catalog projection and public plan together.
        Type itemType = typeof(SqlServer).GetNestedType("ExportCatalogObject", BindingFlags.NonPublic)!;
        object item = Activator.CreateInstance(itemType, nonPublic: true)!;
        foreach (var pair in new Dictionary<string, string> { ["Code"] = code, ["Schema"] = "dbo", ["Name"] = "A_Trigger",
            ["ParentSchema"] = "dbo", ["ParentName"] = "Z_Parent" })
            itemType.GetProperty(pair.Key, BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(item, pair.Value);
        Array catalog = Array.CreateInstance(itemType, 1); catalog.SetValue(item, 0);
        var dependencies = ((IEnumerable<SqlServerDependencyInfo>)typeof(SqlServer)
            .GetMethod("BuildExportTriggerParentDependencies", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, new object[] { catalog })!).ToArray();
        var trigger = Script("A_Trigger", "Module"); trigger.ObjectType = type;
        var plan = SqlServerExportPlan.Create(new[] { trigger, Script("Z_Parent") }, dependencies);
        Assert.Equal(new[] { "Z_Parent", "A_Trigger" }, plan.OrderedScripts.Select(script => script.ObjectName));
        Assert.Equal(new[] { "Table:[dbo].[Z_Parent]" }, plan.Scripts.Single(script => script.ObjectName == "A_Trigger").RequiredScriptIds);
        Assert.Contains(SqlServerExportPlan.Create(new[] { trigger }, dependencies).Issues,
            issue => issue.Kind == SqlServerExportIssueKind.MissingPrerequisite && issue.Detail.Contains("Z_Parent"));
        if (code == "TA") Assert.Contains(plan.Issues, issue => issue.Detail.Contains("CLR assembly"));
    }

    [Fact]
    public void NativeStatementListTransportPreservesLiteralControlCharactersAndEntryBoundaries()
    {
        string[] statements = { "CONSTRAINT [CK] CHECK (N'雪\u001e\r\n<&' <> N'')", "ALTER TABLE [dbo].[A] NOCHECK CONSTRAINT [CK];" };
        string xml = string.Concat(statements.Select(statement => "<item>" + Convert.ToBase64String(Encoding.Unicode.GetBytes(statement)) + "</item>"));
        Assert.Equal(statements, SqlServerManagementMappers.ReadStatementList(xml));
        Assert.Empty(SqlServerManagementMappers.ReadStatementList(""));
    }

    [Fact]
    public void OrdersExpressionAndPostCreateDependenciesWithoutTurningForeignKeyCyclesIntoTableCycles()
    {
        var scripts = new[] { Script("A"), Script("Z"), Script("A", "TablePostCreate"), Script("Z", "TablePostCreate"),
            Script("Fn", "Module"), Script("View", "Module") };
        var dependencies = new[] { Dependency("A", "Fn"), Dependency("View", "A"),
            Dependency("A", "Z", "ForeignKey"), Dependency("Z", "A", "ForeignKey") };
        var plan = SqlServerExportPlan.Create(scripts, dependencies);
        Assert.Empty(plan.Issues);
        string[] order = plan.OrderedScripts.Select(script => script.ObjectName + ":" + script.ScriptType).ToArray();
        Assert.True(Array.IndexOf(order, "Fn:Module") < Array.IndexOf(order, "A:Table"));
        Assert.True(Array.IndexOf(order, "A:Table") < Array.IndexOf(order, "View:Module"));
        Assert.All(plan.OrderedScripts.Where(script => script.ScriptType == "TablePostCreate"), post => {
            Assert.True(Array.IndexOf(order, "A:Table") < Array.IndexOf(order, post.ObjectName + ":TablePostCreate"));
            Assert.True(Array.IndexOf(order, "Z:Table") < Array.IndexOf(order, post.ObjectName + ":TablePostCreate"));
        });
    }

    [Fact]
    public void SnapshotAndFingerprintsAreImmutablePermutationAndCultureIndependent()
    {
        var scripts = new[] { Script("Z"), Script("A") };
        var dependencies = new[] { Dependency("Z", "A"), Dependency("Z", "A") };
        var permission = new SqlServerPermissionInfo { Scope = "Database", PermissionName = "SELECT", GranteeName = "reader" };
        var other = new SqlServerPermissionInfo { Scope = "Database", PermissionName = "UPDATE", GranteeName = "writer" };
        var plan = SqlServerExportPlan.Create(scripts, dependencies, new[] { permission, other }, "Source");
        CultureInfo old = CultureInfo.CurrentCulture;
        try {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");
            var reversed = SqlServerExportPlan.Create(scripts.Reverse(), dependencies.Reverse(), new[] { other, permission }, "Source");
            Assert.Equal(plan.Fingerprint, reversed.Fingerprint);
            Assert.Equal(plan.OrderedScripts.Select(script => script.Id), reversed.OrderedScripts.Select(script => script.Id));
        } finally { CultureInfo.CurrentCulture = old; }
        string fingerprint = plan.Fingerprint;
        scripts[0].Script = "Changed"; permission.PermissionName = "ALTER"; dependencies[0].ReferencedEntityName = "Missing";
        Assert.Equal("SELECT", plan.Permissions.Single(p => p.GranteeName == "reader").PermissionName);
        Assert.DoesNotContain(plan.Scripts, script => script.Script == "Changed");
        Assert.Equal(fingerprint, plan.Fingerprint);
        Assert.NotEqual(fingerprint, SqlServerExportPlan.Create(scripts, dependencies, new[] { permission, other }, "Source").Fingerprint);
        Assert.Throws<NotSupportedException>(() => ((IList<SqlServerExportScript>)plan.Scripts).Clear());
    }

    [Fact]
    public void ExactUtf8HashKeepsSqlLiteralNewlinesAndContentAndNullableMetadataDistinct()
    {
        var script = Script("A"); script.Script = "SELECT N'🙂\r\n雪';";
        var plan = SqlServerExportPlan.Create(new[] { script }, Array.Empty<SqlServerDependencyInfo>());
        string expected = Convert.ToHexString(SHA256.HashData(new UTF8Encoding(false, true).GetBytes(script.Script))).ToLowerInvariant();
        Assert.Equal(expected, plan.Scripts[0].ContentFingerprint);
        script.Script = script.Script.Replace("\r\n", "\n");
        Assert.NotEqual(plan.Fingerprint, SqlServerExportPlan.Create(new[] { script }, Array.Empty<SqlServerDependencyInfo>()).Fingerprint);
        var permission = new SqlServerPermissionInfo();
        var nullable = SqlServerExportPlan.Create(new[] { script }, Array.Empty<SqlServerDependencyInfo>(), new[] { permission });
        permission.SecurableName = "";
        Assert.NotEqual(nullable.Fingerprint, SqlServerExportPlan.Create(new[] { script }, Array.Empty<SqlServerDependencyInfo>(), new[] { permission }).Fingerprint);
    }

    [Fact]
    public void PreservesCaseSensitiveObjectsAndReportsUnresolvedSchemasAndExternalReferences()
    {
        var scripts = new[] { Script("Foo"), Script("foo"), Script("View", "Module") };
        var refs = new[] { Dependency("View", "foo"), Dependency("View", "FOO"), Dependency("View", "Foo") };
        refs[2].ReferencedDatabaseName = "Source";
        var plan = SqlServerExportPlan.Create(scripts, refs, sourceDatabaseName: "Source");
        var view = plan.Scripts.Single(script => script.ObjectName == "View");
        Assert.Equal(new[] { "Table:[dbo].[foo]" }, view.RequiredScriptIds);
        Assert.Contains(plan.Issues, issue => issue.Kind == SqlServerExportIssueKind.MissingPrerequisite);
        Assert.Contains(plan.Issues, issue => issue.Kind == SqlServerExportIssueKind.ExternalDependency);
        refs[0].ReferencedSchemaName = null;
        Assert.Contains(SqlServerExportPlan.Create(scripts, refs).Issues, issue => issue.Detail.Contains("[].[foo]"));
    }

    [Fact]
    public void RetainsCycleBlockedScriptsAndReportsUnavailableUnsupportedAndCallerDependentObjects()
    {
        var missing = Script("Hidden", "Module"); missing.Script = "";
        var unsupported = Script("Ddl", "Module"); unsupported.SchemaName = "";
        var custom = Script("Custom"); custom.SchemaName = "app";
        var call = Dependency("Caller", "Anything"); call.IsCallerDependent = true;
        var plan = SqlServerExportPlan.Create(new[] { Script("A", "Module"), Script("Z", "Module"),
            Script("Downstream", "Module"), Script("Caller", "Module"), missing, unsupported, custom },
            new[] { Dependency("A", "Z"), Dependency("Z", "A"), Dependency("Downstream", "A"), call, Dependency("Caller", "Caller") });
        Assert.Equal(3, plan.Issues.Count(issue => issue.Kind == SqlServerExportIssueKind.CycleOrBlockedDependency));
        Assert.DoesNotContain(plan.OrderedScripts, script => script.ObjectName == "A" || script.ObjectName == "Z" || script.ObjectName == "Downstream");
        Assert.Contains(plan.Scripts, script => script.ObjectName == "Downstream");
        Assert.Contains(plan.Issues, issue => issue.Kind == SqlServerExportIssueKind.UnavailableDefinition);
        Assert.Contains(plan.Issues, issue => issue.Kind == SqlServerExportIssueKind.UnsupportedObject);
        Assert.Contains(plan.Issues, issue => issue.Kind == SqlServerExportIssueKind.UnresolvedDependency);
        Assert.Contains(plan.Issues, issue => issue.Kind == SqlServerExportIssueKind.MissingPrerequisite);
        Assert.Empty(plan.Scripts.Single(script => script.ObjectName == "Caller").RequiredScriptIds);
    }

    [Fact]
    public void RejectsConflictingIdentitiesRatherThanSilentlyDiscardingScripts()
    {
        Assert.Throws<ArgumentException>(() => SqlServerExportPlan.Create(new[] { Script("A"), Script("A") }, Array.Empty<SqlServerDependencyInfo>()));
        Assert.Throws<ArgumentException>(() => SqlServerExportPlan.Create(new[] { Script("A"), Script("A", "Module") }, Array.Empty<SqlServerDependencyInfo>()));
        Assert.Throws<ArgumentNullException>(() => SqlServerExportPlan.Create(null!, Array.Empty<SqlServerDependencyInfo>()));
    }

    private static SqlServerScriptInfo Script(string name, string family = "Table")
        => new() { ScriptType = family, SchemaName = "dbo", ObjectName = name,
            ObjectType = family == "Module" ? "VIEW" : "USER_TABLE", Script = "-- " + name };
    private static SqlServerDependencyInfo Dependency(string source, string target, string kind = "SqlExpression")
        => new() { DependencyType = kind, ReferencingSchema = "dbo", ReferencingName = source,
            ReferencedSchemaName = "dbo", ReferencedEntityName = target, ReferencedClassDescription = "OBJECT_OR_COLUMN" };
}
