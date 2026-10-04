using System.Data;
using System.Diagnostics;
using System.Text;
using DBAClientX;
using DBAClientX.SqlServerManagement;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed class SqlServerExportNativeTests
{
    [Fact]
    [Trait("Category", "LiveSqlExport")]
    public async Task OrderedCaseSensitiveScriptsRecreateObjectsAndKeepPermissionsAndOmissionsExplicit()
    {
        var settings = Settings();
        await using var source = new SqlServerRecoveryTestScope(settings.Connection, settings.Directory);
        await using var target = new SqlServerRecoveryTestScope(settings.Connection, settings.Directory);
        await using var admin = new SqlConnection(settings.Connection);
        await admin.OpenAsync();
        await source.CreateSourceAsync(admin);
        await target.CreateSourceAsync(admin);
        await Execute(admin, $"ALTER DATABASE [{source.SourceName}] COLLATE Latin1_General_100_CS_AS; ALTER DATABASE [{target.SourceName}] COLLATE Latin1_General_100_CS_AS;");
        string connectionString = new SqlConnectionStringBuilder(settings.Connection) { InitialCatalog = source.SourceName }.ConnectionString;
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        foreach (string statement in new[] {
            "CREATE TABLE dbo.Foo(Id int NOT NULL PRIMARY KEY)",
            "ALTER TABLE dbo.Foo ADD CONSTRAINT CK_Control CHECK (N'left\u001eright' = N'left\u001eright')",
            "CREATE TABLE dbo.foo(Id int NOT NULL PRIMARY KEY)",
            "CREATE FUNCTION dbo.zDefault(@input int) RETURNS int WITH SCHEMABINDING AS BEGIN RETURN 42; END",
            "CREATE TABLE dbo.aDefault(Value int NOT NULL DEFAULT (dbo.zDefault(0)))",
            "CREATE TABLE dbo.aComputed(Input int NULL, Value AS (dbo.zDefault(Input)))",
            "CREATE VIEW dbo.aView AS SELECT Id FROM dbo.Foo",
            "CREATE FUNCTION dbo.zViewFunction() RETURNS TABLE AS RETURN SELECT Id FROM dbo.aView",
            "CREATE PROCEDURE dbo.aProc AS SELECT Id FROM dbo.zViewFunction()",
            "CREATE TRIGGER dbo.aTrigger ON dbo.Foo AFTER INSERT, DELETE AS BEGIN INSERT dbo.foo SELECT Id FROM inserted; DELETE f FROM dbo.foo f JOIN deleted d ON d.Id = f.Id; END",
            "CREATE TABLE dbo.A(Id int NOT NULL PRIMARY KEY, OtherId int NULL)",
            "CREATE FUNCTION dbo.zCheck(@value int) RETURNS int AS BEGIN RETURN (SELECT COUNT(*) FROM dbo.A); END",
            "ALTER TABLE dbo.A WITH NOCHECK ADD CONSTRAINT CK_A CHECK (dbo.zCheck(Id) >= 0)",
            "CREATE TABLE dbo.Z(Id int NOT NULL PRIMARY KEY, OtherId int NULL REFERENCES dbo.A(Id))",
            "ALTER TABLE dbo.A ADD CONSTRAINT FK_A_Z FOREIGN KEY(OtherId) REFERENCES dbo.Z(Id)",
            "CREATE USER ExportReader WITHOUT LOGIN; GRANT SELECT ON dbo.Foo TO ExportReader; DENY UPDATE ON dbo.Foo TO ExportReader"
        }) await Execute(connection, statement);

        using var client = new SqlServer();
        var plan = Capture(client, connectionString);
        Assert.Empty(plan.Issues);
        Assert.Equal(source.SourceName, plan.SourceDatabaseName);
        Assert.Equal(plan.Fingerprint, client.GetSqlServerExportPlan(connectionString).Fingerprint);
        Assert.Contains(plan.Scripts, script => script.ObjectName == "Foo");
        Assert.Contains(plan.Scripts, script => script.ObjectName == "foo");
        Assert.Contains("left\u001eright", plan.Scripts.Single(script => script.ObjectName == "Foo").Script);
        Assert.Contains(plan.Permissions, permission => permission.GranteeName == "ExportReader" && permission.State == "D" && permission.PermissionName == "UPDATE");
        Assert.All(plan.Permissions, permission => Assert.Equal("Database", permission.Scope));
        Assert.Equal(new[] { "Module:[dbo].[zDefault]" }, plan.Scripts.Single(script => script.ObjectName == "aDefault").RequiredScriptIds);
        Assert.Contains("Table:[dbo].[Foo]", plan.Scripts.Single(script => script.ObjectName == "aTrigger").RequiredScriptIds);
        Assert.Contains("Module:[dbo].[zCheck]", plan.Scripts.Single(script => script.ObjectName == "A" && script.ScriptType == "TablePostCreate").RequiredScriptIds);
        Assert.Contains(client.GetSqlServerExportPlan(connectionString, name: "aProc").Issues,
            issue => issue.Kind == SqlServerExportIssueKind.MissingPrerequisite);

        // sqlcmd is an optional test-only native batch reader, never a production dependency of the plan.
        string scriptPath = Path.Combine(source.BackupDirectory, "ordered.sql");
        await File.WriteAllTextAsync(scriptPath, string.Join("\r\nGO\r\n", plan.OrderedScripts.Select(script => script.Script)), Encoding.Unicode);
        var start = new ProcessStartInfo("sqlcmd") { UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, CreateNoWindow = true };
        foreach (string argument in new[] { "-S", "localhost", "-E", "-C", "-b", "-d", target.SourceName, "-i", scriptPath }) start.ArgumentList.Add(argument);
        using var process = Process.Start(start)!;
        Task<string> output = process.StandardOutput.ReadToEndAsync();
        Task<string> error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        try { await process.WaitForExitAsync(timeout.Token); }
        finally { if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); } }
        Assert.True(process.ExitCode == 0, await output + await error);
        await using var restored = new SqlConnection(new SqlConnectionStringBuilder(settings.Connection) { InitialCatalog = target.SourceName }.ConnectionString);
        await restored.OpenAsync();
        await Execute(restored, "INSERT dbo.Foo VALUES(7); INSERT dbo.aDefault DEFAULT VALUES; INSERT dbo.aComputed DEFAULT VALUES;");
        using var query = new SqlCommand("SELECT (SELECT Id FROM dbo.foo), (SELECT Value FROM dbo.aDefault), (SELECT Value FROM dbo.aComputed); EXEC dbo.aProc", restored);
        await using var reader = await query.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync()); Assert.Equal(7, reader.GetInt32(0)); Assert.Equal(42, reader.GetInt32(1)); Assert.Equal(42, reader.GetInt32(2));
        Assert.True(await reader.NextResultAsync()); Assert.True(await reader.ReadAsync()); Assert.Equal(7, reader.GetInt32(0));
        await reader.DisposeAsync();

        await Execute(connection, "CREATE PROCEDURE dbo.Hidden WITH ENCRYPTION AS SELECT 1");
        await Execute(connection, "CREATE SYNONYM dbo.RemoteName FOR other_database.dbo.RemoteObject");
        await Execute(connection, "CREATE PROCEDURE dbo.RemoteProc AS SELECT * FROM other_database.dbo.RemoteObject");
        var incomplete = client.GetSqlServerExportPlan(connectionString);
        Assert.Contains(incomplete.Issues, issue => issue.Kind == SqlServerExportIssueKind.UnavailableDefinition && issue.ObjectName == "[dbo].[Hidden]");
        Assert.Contains(incomplete.Issues, issue => issue.Kind == SqlServerExportIssueKind.UnsupportedObject && issue.ObjectName == "[dbo].[RemoteName]");
        Assert.Contains(incomplete.Issues, issue => issue.Kind == SqlServerExportIssueKind.ExternalDependency && issue.ObjectName.Contains("RemoteProc"));
        Assert.DoesNotContain(incomplete.Scripts, script => script.ObjectName == "Hidden");
    }

    [Fact]
    [Trait("Category", "LiveSqlExport")]
    public async Task FilteredGraphExportReportsEndpointPrerequisites()
    {
        var settings = Settings();
        await using var scope = new SqlServerRecoveryTestScope(settings.Connection, settings.Directory);
        await using var admin = new SqlConnection(settings.Connection); await admin.OpenAsync(); await scope.CreateSourceAsync(admin);
        string connectionString = new SqlConnectionStringBuilder(settings.Connection) { InitialCatalog = scope.SourceName }.ConnectionString;
        await using var connection = new SqlConnection(connectionString); await connection.OpenAsync();
        await Execute(connection, "CREATE TABLE dbo.ZFrom(Id int) AS NODE; CREATE TABLE dbo.ZTo(Id int) AS NODE; CREATE TABLE dbo.AEdge AS EDGE; "
            + "ALTER TABLE dbo.AEdge ADD CONSTRAINT EC_Connection CONNECTION (dbo.ZFrom TO dbo.ZTo)");
        using var client = new SqlServer();
        var filtered = client.GetSqlServerExportPlan(connectionString, name: "AEdge");
        Assert.Contains(filtered.Issues, issue => issue.Kind == SqlServerExportIssueKind.MissingPrerequisite && issue.Detail.Contains("[dbo].[ZFrom]"));
        Assert.Contains(filtered.Issues, issue => issue.Kind == SqlServerExportIssueKind.MissingPrerequisite && issue.Detail.Contains("[dbo].[ZTo]"));
        var complete = client.GetSqlServerExportPlan(connectionString);
        Assert.Empty(complete.Issues);
        var post = complete.Scripts.Single(script => script.ObjectName == "AEdge" && script.ScriptType == "TablePostCreate");
        Assert.Contains("Table:[dbo].[ZFrom]", post.RequiredScriptIds); Assert.Contains("Table:[dbo].[ZTo]", post.RequiredScriptIds);
    }

    [Fact]
    [Trait("Category", "LiveSqlExport")]
    public async Task NativeResolutionUsesCatalogCaseAndSchemaAndRefusesInvisibleDefinitions()
    {
        var settings = Settings();
        await using var scope = new SqlServerRecoveryTestScope(settings.Connection, settings.Directory);
        await using var admin = new SqlConnection(settings.Connection);
        await admin.OpenAsync(); await scope.CreateSourceAsync(admin);
        await Execute(admin, $"ALTER DATABASE [{scope.SourceName}] COLLATE Latin1_General_100_CI_AS");
        string connectionString = new SqlConnectionStringBuilder(settings.Connection) { InitialCatalog = scope.SourceName }.ConnectionString;
        await using var connection = new SqlConnection(connectionString); await connection.OpenAsync();
        await Execute(connection, "CREATE TABLE dbo.CatalogName(Value int)");
        await Execute(connection, "CREATE VIEW dbo.ViewName AS SELECT Value FROM CATALOGNAME");
        await Execute(connection, "CREATE USER ExportRestricted WITHOUT LOGIN; DENY VIEW DEFINITION TO ExportRestricted");
        using var client = new SqlServer();
        var plan = Capture(client, connectionString);
        Assert.Empty(plan.Issues);
        Assert.Equal(new[] { "Table:[dbo].[CatalogName]" }, plan.Scripts.Single(script => script.ObjectName == "ViewName").RequiredScriptIds);
        var legacy = client.GetSqlServerDependencies(connectionString).Single(dependency => dependency.ReferencingName == "ViewName");
        Assert.Equal("CATALOGNAME", legacy.ReferencedEntityName); Assert.Null(legacy.ReferencedSchemaName);
        using var restricted = new SqlServer { ConnectionOptions = new SqlServerConnectionOptions { ConnectionFactory = text => {
            var owned = new SqlConnection(text);
            owned.StateChange += (_, state) => {
                if (state.OriginalState == ConnectionState.Closed && state.CurrentState == ConnectionState.Open)
                    using (var impersonate = new SqlCommand("EXECUTE AS USER = 'ExportRestricted'", owned)) impersonate.ExecuteNonQuery();
            };
            return owned;
        } } };
        Assert.Throws<UnauthorizedAccessException>(() => restricted.GetSqlServerExportPlan(connectionString));
        await Execute(connection, "CREATE USER ExportAllowed WITHOUT LOGIN; GRANT VIEW DEFINITION TO ExportAllowed; "
            + "GRANT SELECT ON sys.sql_expression_dependencies TO ExportAllowed");
        using var allowed = new SqlServer { ConnectionOptions = new SqlServerConnectionOptions { ConnectionFactory = text => {
            var owned = new SqlConnection(text);
            owned.StateChange += (_, state) => {
                if (state.OriginalState == ConnectionState.Closed && state.CurrentState == ConnectionState.Open)
                    using (var impersonate = new SqlCommand("EXECUTE AS USER = 'ExportAllowed'", owned)) impersonate.ExecuteNonQuery();
            };
            return owned;
        } } };
        var leastPrivilege = allowed.GetSqlServerExportPlan(connectionString);
        Assert.Empty(leastPrivilege.Issues);
        Assert.Equal(plan.Scripts.Select(script => script.Id), leastPrivilege.Scripts.Select(script => script.Id));
        Assert.All(leastPrivilege.Permissions, permission => Assert.Equal("Database", permission.Scope));
        await Execute(connection, "DENY SELECT ON sys.sql_expression_dependencies TO ExportAllowed");
        Assert.Throws<UnauthorizedAccessException>(() => allowed.GetSqlServerExportPlan(connectionString));
    }

    private static (string Connection, string Directory) Settings()
    {
        string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_EXPORT_TEST_CONNECTION");
        string? directory = Environment.GetEnvironmentVariable("DBACLIENTX_SQL_EXPORT_TEST_DIRECTORY");
        Assert.SkipWhen(!OperatingSystem.IsWindows() || string.IsNullOrWhiteSpace(connection) || string.IsNullOrWhiteSpace(directory),
            "Set local DBACLIENTX_SQL_EXPORT_TEST_CONNECTION and DIRECTORY for uniquely owned native export qualification; sqlcmd must be installed.");
        var builder = new SqlConnectionStringBuilder(connection) { InitialCatalog = "master", Pooling = false, Enlist = false };
        Assert.Equal("localhost", builder.DataSource); Assert.True(builder.IntegratedSecurity);
        Assert.True(System.IO.Directory.Exists(directory));
        return (builder.ConnectionString, directory!);
    }
    private static async Task Execute(SqlConnection connection, string sql)
    {
        using var command = new SqlCommand(sql, connection);
        try { await command.ExecuteNonQueryAsync(); }
        catch (SqlException exception) { throw new InvalidOperationException("Native fixture statement failed: " + sql, exception); }
    }
    private static SqlServerExportPlan Capture(SqlServer client, string connection)
    {
        try { return client.GetSqlServerExportPlan(connection); }
        catch (SqlException exception) { throw new InvalidOperationException(string.Join("; ", exception.Errors.Cast<SqlError>()
            .Select(error => $"SQL {error.Number} at line {error.LineNumber}: {error.Message}")), exception); }
    }
}
