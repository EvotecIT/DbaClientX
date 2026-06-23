using System;
using System.Collections.Generic;
using System.Data;
using DBAClientX.Metadata;
using DBAClientX.SqlServerManagement;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerManagementTests
{
    [Fact]
    public void ParseAgentDateAndTime_ConvertsSqlAgentIntegers()
    {
        Assert.Equal(new DateTime(2026, 6, 23), SqlServerManagementMappers.ParseAgentDate(20260623));
        Assert.Equal(new TimeSpan(14, 5, 7), SqlServerManagementMappers.ParseAgentTime(140507));
        Assert.Null(SqlServerManagementMappers.ParseAgentDate(0));
    }

    [Fact]
    public void MapAgentJob_MapsDefinitionFields()
    {
        Guid jobId = Guid.NewGuid();
        using var reader = ReadSingleRow(
            ("JobId", typeof(Guid), jobId),
            ("Name", typeof(string), "Nightly backup"),
            ("Category", typeof(string), "Database Maintenance"),
            ("OwnerLoginName", typeof(string), "sa"),
            ("Description", typeof(string), "Backs up user databases."),
            ("Enabled", typeof(bool), true),
            ("Created", typeof(DateTime), new DateTime(2026, 1, 2)),
            ("Modified", typeof(DateTime), new DateTime(2026, 1, 3)));

        SqlServerAgentJobInfo job = SqlServerManagementMappers.MapAgentJob(reader);

        Assert.Equal(jobId, job.JobId);
        Assert.Equal("Nightly backup", job.Name);
        Assert.Equal("Database Maintenance", job.Category);
        Assert.True(job.Enabled);
        Assert.Equal(new DateTime(2026, 1, 3), job.Modified);
    }

    [Fact]
    public void MapPrincipal_MapsSidAsHexString()
    {
        using var reader = ReadSingleRow(
            ("Scope", typeof(string), "Server"),
            ("DatabaseName", typeof(string), DBNull.Value),
            ("Name", typeof(string), "app_login"),
            ("Type", typeof(string), "S"),
            ("TypeDescription", typeof(string), "SQL_LOGIN"),
            ("Sid", typeof(byte[]), new byte[] { 0x01, 0xAB }),
            ("DefaultDatabaseName", typeof(string), "AppDb"),
            ("DefaultSchemaName", typeof(string), DBNull.Value),
            ("AuthenticationType", typeof(string), DBNull.Value),
            ("IsDisabled", typeof(bool), false),
            ("IsFixedRole", typeof(bool), false),
            ("Created", typeof(DateTime), new DateTime(2026, 2, 1)),
            ("Modified", typeof(DateTime), new DateTime(2026, 2, 2)));

        SqlServerPrincipalInfo principal = SqlServerManagementMappers.MapPrincipal(reader);

        Assert.Equal("Server", principal.Scope);
        Assert.Equal("app_login", principal.Name);
        Assert.Equal("0x01AB", principal.Sid);
        Assert.False(principal.IsDisabled);
    }

    [Fact]
    public void MapPermission_MapsSecurableAndPrincipals()
    {
        using var reader = ReadSingleRow(
            ("Scope", typeof(string), "Database"),
            ("DatabaseName", typeof(string), "AppDb"),
            ("State", typeof(string), "G"),
            ("StateDescription", typeof(string), "GRANT"),
            ("PermissionName", typeof(string), "SELECT"),
            ("ClassDescription", typeof(string), "OBJECT_OR_COLUMN"),
            ("SecurableSchema", typeof(string), "dbo"),
            ("SecurableName", typeof(string), "Users"),
            ("SecurableColumn", typeof(string), "DisplayName"),
            ("GranteeName", typeof(string), "app_role"),
            ("GrantorName", typeof(string), "dbo"));

        SqlServerPermissionInfo permission = SqlServerManagementMappers.MapPermission(reader);

        Assert.Equal("Database", permission.Scope);
        Assert.Equal("SELECT", permission.PermissionName);
        Assert.Equal("dbo", permission.SecurableSchema);
        Assert.Equal("Users", permission.SecurableName);
        Assert.Equal("DisplayName", permission.SecurableColumn);
        Assert.Equal("app_role", permission.GranteeName);
    }

    [Fact]
    public void MapDependency_MapsCrossDatabaseReference()
    {
        using var reader = ReadSingleRow(
            ("DependencyType", typeof(string), "SqlExpression"),
            ("ReferencingSchema", typeof(string), "dbo"),
            ("ReferencingName", typeof(string), "ViewUsers"),
            ("ReferencingType", typeof(string), "VIEW"),
            ("ReferencedServerName", typeof(string), DBNull.Value),
            ("ReferencedDatabaseName", typeof(string), "OtherDb"),
            ("ReferencedSchemaName", typeof(string), "dbo"),
            ("ReferencedEntityName", typeof(string), "Users"),
            ("ReferencedClassDescription", typeof(string), "OBJECT_OR_COLUMN"),
            ("IsCallerDependent", typeof(bool), false),
            ("IsAmbiguous", typeof(bool), true));

        SqlServerDependencyInfo dependency = SqlServerManagementMappers.MapDependency(reader);

        Assert.Equal("SqlExpression", dependency.DependencyType);
        Assert.Equal("ViewUsers", dependency.ReferencingName);
        Assert.Equal("OtherDb", dependency.ReferencedDatabaseName);
        Assert.True(dependency.IsAmbiguous);
    }

    [Fact]
    public void NormalizeModuleScript_RewritesLeadingAlter()
    {
        string script = SqlServerManagementMappers.NormalizeModuleScript("  ALTER PROCEDURE [dbo].[DoWork] AS SELECT 1;");

        Assert.StartsWith("CREATE OR ALTER PROCEDURE", script.TrimStart(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildTableScripts_GeneratesQuotedCreateTable()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Id",
                Ordinal = 1,
                DataType = "int",
                IsIdentity = true,
                IdentitySeed = "1",
                IdentityIncrement = "1",
                PrimaryKeyName = "PK_UserAudit",
                PrimaryKeyOrdinal = 1,
                PrimaryKeyIndexType = "CLUSTERED",
                PrimaryKeyIsDescending = false
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Name",
                Ordinal = 2,
                DataType = "nvarchar(128) COLLATE Polish_CI_AS",
                IsNullable = false,
                DefaultDefinition = "N''"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Ratio",
                Ordinal = 3,
                DataType = "float(24)",
                IsNullable = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Payload",
                Ordinal = 4,
                DataType = "xml(DOCUMENT [dbo].[AuditPayload])",
                IsNullable = true
            }
        };

        IReadOnlyList<SqlServerScriptInfo> scripts = SqlServerManagementScripting.BuildTableScripts(columns);

        SqlServerScriptInfo script = Assert.Single(scripts);
        Assert.Equal("Table", script.ScriptType);
        Assert.Contains("CREATE TABLE [dbo].[User]]Audit]", script.Script);
        Assert.Contains("[Id] int IDENTITY(1,1) NOT NULL", script.Script);
        Assert.Contains("[Name] nvarchar(128) COLLATE Polish_CI_AS NOT NULL DEFAULT N''", script.Script);
        Assert.Contains("[Ratio] float(24) NULL", script.Script);
        Assert.Contains("[Payload] xml(DOCUMENT [dbo].[AuditPayload]) NULL", script.Script);
        Assert.Contains("CONSTRAINT [PK_UserAudit] PRIMARY KEY CLUSTERED ([Id] ASC)", script.Script);
    }

    [Fact]
    public void BuildTableCopyPlan_InfersPrimaryKeyAndCommands()
    {
        var columns = new[]
        {
            new DbaColumnInfo("dbo", "Users", "Id", "int") { Ordinal = 1, IsNullable = false },
            new DbaColumnInfo("dbo", "Users", "DisplayName", "nvarchar(128)") { Ordinal = 2, IsNullable = true }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "Id", Ordinal = 1, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Users",
            "archive",
            "Users",
            columns,
            indexes);

        Assert.Equal("SELECT [Id], [DisplayName] FROM [dbo].[Users];", plan.SourceSelectCommand);
        Assert.Equal("INSERT INTO [archive].[Users] ([Id], [DisplayName]) VALUES (@p0, @p1);", plan.DestinationInsertCommand);
        Assert.Equal("Id", Assert.Single(plan.KeyColumns));
        Assert.NotNull(plan.DestinationMergeCommand);
        Assert.Contains("ON target.[Id] = source.[Id]", plan.DestinationMergeCommand);
        Assert.Contains("UPDATE SET target.[DisplayName] = source.[DisplayName]", plan.DestinationMergeCommand);
    }

    [Fact]
    public void BuildTableCopyPlan_SkipsComputedColumns()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "Id",
                Ordinal = 1,
                DataType = "int"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "DisplayName",
                Ordinal = 2,
                DataType = "nvarchar(128)"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "SearchName",
                Ordinal = 3,
                DataType = "nvarchar(128)",
                ComputedDefinition = "lower([DisplayName])"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "RowVersion",
                Ordinal = 4,
                DataType = "rowversion"
            }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "Id", Ordinal = 1, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Users",
            "dbo",
            "Users",
            columns,
            indexes);

        Assert.DoesNotContain(plan.Columns, column => column.SourceColumn == "SearchName");
        Assert.DoesNotContain(plan.Columns, column => column.SourceColumn == "RowVersion");
        Assert.DoesNotContain("[SearchName]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[RowVersion]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[SearchName]", plan.DestinationMergeCommand ?? string.Empty);
        Assert.DoesNotContain("[RowVersion]", plan.DestinationMergeCommand ?? string.Empty);
    }

    [Fact]
    public void BuildTableCopyPlan_WrapsIdentityInsertCommands()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "Id",
                Ordinal = 1,
                DataType = "int",
                IsIdentity = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "DisplayName",
                Ordinal = 2,
                DataType = "nvarchar(128)"
            }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "Id", Ordinal = 1, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Users",
            "archive",
            "Users",
            columns,
            indexes);

        Assert.True(plan.RequiresIdentityInsert);
        Assert.True(Assert.Single(plan.Columns, column => column.SourceColumn == "Id").IsIdentity);
        Assert.StartsWith("SET IDENTITY_INSERT [archive].[Users] ON; INSERT INTO", plan.DestinationInsertCommand);
        Assert.EndsWith("SET IDENTITY_INSERT [archive].[Users] OFF;", plan.DestinationInsertCommand);
        Assert.Contains("SET IDENTITY_INSERT [archive].[Users] ON; MERGE", plan.DestinationMergeCommand);
    }

    private static DataTableReader ReadSingleRow(params (string Name, Type Type, object Value)[] columns)
    {
        var table = new DataTable();
        foreach ((string name, Type type, _) in columns)
        {
            table.Columns.Add(name, type);
        }

        DataRow row = table.NewRow();
        foreach ((string name, _, object value) in columns)
        {
            row[name] = value;
        }

        table.Rows.Add(row);
        DataTableReader reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }
}
