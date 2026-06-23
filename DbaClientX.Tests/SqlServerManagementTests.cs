using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using DBAClientX;
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
            ("StartStepId", typeof(int), 2),
            ("Created", typeof(DateTime), new DateTime(2026, 1, 2)),
            ("Modified", typeof(DateTime), new DateTime(2026, 1, 3)));

        SqlServerAgentJobInfo job = SqlServerManagementMappers.MapAgentJob(reader);

        Assert.Equal(jobId, job.JobId);
        Assert.Equal("Nightly backup", job.Name);
        Assert.Equal("Database Maintenance", job.Category);
        Assert.True(job.Enabled);
        Assert.Equal(2, job.StartStepId);
        Assert.Equal(new DateTime(2026, 1, 3), job.Modified);
    }

    [Fact]
    public void MapAgentJobStep_MapsGotoStepTargets()
    {
        Guid jobId = Guid.NewGuid();
        using var reader = ReadSingleRow(
            ("JobId", typeof(Guid), jobId),
            ("JobName", typeof(string), "Nightly backup"),
            ("StepId", typeof(int), 1),
            ("StepName", typeof(string), "Run task"),
            ("Subsystem", typeof(string), "TSQL"),
            ("Command", typeof(string), "SELECT 1"),
            ("DatabaseName", typeof(string), "master"),
            ("OnSuccessAction", typeof(string), "GoToStep"),
            ("OnSuccessStepId", typeof(int), 3),
            ("OnFailAction", typeof(string), "GoToStep"),
            ("OnFailStepId", typeof(int), 4),
            ("RetryAttempts", typeof(int), 2),
            ("RetryInterval", typeof(int), 5));

        SqlServerAgentJobStepInfo step = SqlServerManagementMappers.MapAgentJobStep(reader);

        Assert.Equal(jobId, step.JobId);
        Assert.Equal("GoToStep", step.OnSuccessAction);
        Assert.Equal(3, step.OnSuccessStepId);
        Assert.Equal("GoToStep", step.OnFailAction);
        Assert.Equal(4, step.OnFailStepId);
    }

    [Fact]
    public void MapAgentSchedule_MapsRecurrenceFields()
    {
        Guid jobId = Guid.NewGuid();
        using var reader = ReadSingleRow(
            ("JobId", typeof(Guid), jobId),
            ("JobName", typeof(string), "Nightly backup"),
            ("ScheduleId", typeof(int), 7),
            ("Name", typeof(string), "Every other Tuesday"),
            ("Enabled", typeof(bool), true),
            ("FrequencyType", typeof(int), 32),
            ("FrequencyInterval", typeof(int), 3),
            ("FrequencyRelativeInterval", typeof(int), 2),
            ("FrequencySubdayType", typeof(int), 4),
            ("FrequencySubdayInterval", typeof(int), 15),
            ("FrequencyRecurrenceFactor", typeof(int), 2),
            ("ActiveStartDate", typeof(int), 20260623),
            ("ActiveEndDate", typeof(int), 0),
            ("ActiveStartTime", typeof(int), 130000),
            ("ActiveEndTime", typeof(int), 235959));

        SqlServerAgentScheduleInfo schedule = SqlServerManagementMappers.MapAgentSchedule(reader);

        Assert.Equal(jobId, schedule.JobId);
        Assert.Equal(2, schedule.FrequencyRelativeInterval);
        Assert.Equal(2, schedule.FrequencyRecurrenceFactor);
        Assert.Equal(new DateTime(2026, 6, 23), schedule.ActiveStartDate);
        Assert.Null(schedule.ActiveEndDate);
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
            ("ClassDescription", typeof(string), "TYPE"),
            ("SecurableSchema", typeof(string), "dbo"),
            ("SecurableName", typeof(string), "PhoneNumber"),
            ("SecurableColumn", typeof(string), DBNull.Value),
            ("GranteeName", typeof(string), "app_role"),
            ("GrantorName", typeof(string), "dbo"));

        SqlServerPermissionInfo permission = SqlServerManagementMappers.MapPermission(reader);

        Assert.Equal("Database", permission.Scope);
        Assert.Equal("SELECT", permission.PermissionName);
        Assert.Equal("dbo", permission.SecurableSchema);
        Assert.Equal("PhoneNumber", permission.SecurableName);
        Assert.Null(permission.SecurableColumn);
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
        string script = SqlServerManagementMappers.NormalizeModuleScript("SET ANSI_NULLS ON;\r\nGO\r\nSET QUOTED_IDENTIFIER ON;\r\nGO\r\n  ALTER PROCEDURE [dbo].[DoWork] AS BEGIN ALTER TABLE [dbo].[T] ADD [X] int; END;");

        Assert.Contains("SET ANSI_NULLS ON;", script);
        Assert.Contains("GO\r\nSET QUOTED_IDENTIFIER ON;", script);
        Assert.Contains("CREATE OR ALTER PROCEDURE [dbo].[DoWork]", script);
        Assert.Contains("ALTER TABLE [dbo].[T]", script);
    }

    [Fact]
    public void BuildTableScripts_GeneratesQuotedCreateTable()
    {
        string separator = SqlServerManagementScripting.ConstraintDefinitionSeparator.ToString();
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
                IdentityNotForReplication = true,
                AdditionalConstraintDefinitions = string.Join(
                    separator,
                    "CONSTRAINT [CK_UserAudit_Name] CHECK ([Name]\r\n <> N'')",
                    "CONSTRAINT [CK_UserAudit_Code] CHECK ([Code] <> N'')",
                    "CONSTRAINT [CK_UserAudit_code] CHECK ([code] <> N'')"),
                PostCreateStatements = string.Join(
                    separator,
                    "ALTER TABLE [dbo].[User]]Audit] ADD CONSTRAINT [FK_UserAudit_Role] FOREIGN KEY ([Id]) REFERENCES [dbo].[Roles] ([Id]) ON DELETE CASCADE NOT FOR REPLICATION;",
                    "ALTER TABLE [dbo].[User]]Audit] WITH NOCHECK ADD CONSTRAINT [CK_Disabled] CHECK ([Id] > 0);",
                    "ALTER TABLE [dbo].[User]]Audit] NOCHECK CONSTRAINT [CK_Disabled];"),
                PrimaryKeyName = "PK_UserAudit",
                PrimaryKeyOrdinal = 1,
                PrimaryKeyIndexType = "CLUSTERED",
                PrimaryKeyIsDescending = false
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "RowGuid",
                Ordinal = 2,
                DataType = "uniqueidentifier",
                IsNullable = false,
                IsRowGuidColumn = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Name",
                Ordinal = 3,
                DataType = "nvarchar(128) COLLATE Polish_CI_AS",
                IsNullable = false,
                DefaultConstraintName = "DF_UserAudit_Name",
                DefaultDefinition = "N''",
                UniqueConstraintName = "UQ_UserAudit_Name",
                UniqueConstraintOrdinal = 1,
                UniqueConstraintIndexType = "NONCLUSTERED",
                UniqueConstraintIsDescending = false
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Ratio",
                Ordinal = 4,
                DataType = "float(24)",
                IsNullable = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Payload",
                Ordinal = 5,
                DataType = "xml(DOCUMENT [dbo].[AuditPayload])",
                IsNullable = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Blob",
                Ordinal = 6,
                DataType = "varbinary(max) FILESTREAM",
                IsNullable = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "SparseCode",
                Ordinal = 7,
                DataType = "int",
                IsNullable = true,
                IsSparse = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Email",
                Ordinal = 8,
                DataType = "nvarchar(256)",
                IsNullable = true,
                MaskingFunction = "email()"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "NameLength",
                Ordinal = 9,
                DataType = "int",
                IsNullable = false,
                ComputedDefinition = "(len([Name]))",
                IsPersisted = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "ValidFrom",
                Ordinal = 10,
                DataType = "datetime2(7)",
                IsNullable = false,
                GeneratedAlwaysTypeDescription = "AS_ROW_START",
                IsHidden = true,
                TemporalType = 2,
                HistoryTableSchema = "history",
                HistoryTableName = "UserAuditHistory"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "ValidTo",
                Ordinal = 11,
                DataType = "datetime2(7)",
                IsNullable = false,
                GeneratedAlwaysTypeDescription = "AS_ROW_END",
                IsHidden = true,
                TemporalType = 2,
                HistoryTableSchema = "history",
                HistoryTableName = "UserAuditHistory"
            }
        };

        IReadOnlyList<SqlServerScriptInfo> scripts = SqlServerManagementScripting.BuildTableScripts(columns);

        Assert.Equal(2, scripts.Count);
        SqlServerScriptInfo script = Assert.Single(scripts, item => item.ScriptType == "Table");
        SqlServerScriptInfo postCreateScript = Assert.Single(scripts, item => item.ScriptType == "TablePostCreate");
        Assert.Equal("Table", script.ScriptType);
        Assert.Contains("CREATE TABLE [dbo].[User]]Audit]", script.Script);
        Assert.Contains("[Id] int IDENTITY(1,1) NOT FOR REPLICATION NOT NULL", script.Script);
        Assert.Contains("[RowGuid] uniqueidentifier ROWGUIDCOL NOT NULL", script.Script);
        Assert.Contains("[Name] nvarchar(128) COLLATE Polish_CI_AS NOT NULL CONSTRAINT [DF_UserAudit_Name] DEFAULT N''", script.Script);
        Assert.Contains("[Ratio] float(24) NULL", script.Script);
        Assert.Contains("[Payload] xml(DOCUMENT [dbo].[AuditPayload]) NULL", script.Script);
        Assert.Contains("[Blob] varbinary(max) FILESTREAM NULL", script.Script);
        Assert.Contains("[SparseCode] int SPARSE NULL", script.Script);
        Assert.Contains("[Email] nvarchar(256) MASKED WITH (FUNCTION = N'email()') NULL", script.Script);
        Assert.Contains("[NameLength] AS (len([Name])) PERSISTED NOT NULL", script.Script);
        Assert.Contains("[ValidFrom] datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL", script.Script);
        Assert.Contains("[ValidTo] datetime2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL", script.Script);
        Assert.Contains("CONSTRAINT [PK_UserAudit] PRIMARY KEY CLUSTERED ([Id] ASC)", script.Script);
        Assert.Contains("CONSTRAINT [UQ_UserAudit_Name] UNIQUE NONCLUSTERED ([Name] ASC)", script.Script);
        Assert.Contains("CONSTRAINT [CK_UserAudit_Name] CHECK ([Name]\r\n <> N'')", script.Script);
        Assert.Contains("CONSTRAINT [CK_UserAudit_Code] CHECK ([Code] <> N'')", script.Script);
        Assert.Contains("CONSTRAINT [CK_UserAudit_code] CHECK ([code] <> N'')", script.Script);
        Assert.DoesNotContain("FOREIGN KEY", script.Script);
        Assert.Contains("PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])", script.Script);
        Assert.Contains("WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [history].[UserAuditHistory]));", script.Script);
        Assert.Contains("ALTER TABLE [dbo].[User]]Audit] ADD CONSTRAINT [FK_UserAudit_Role] FOREIGN KEY ([Id]) REFERENCES [dbo].[Roles] ([Id]) ON DELETE CASCADE NOT FOR REPLICATION;", postCreateScript.Script);
        Assert.Contains("ALTER TABLE [dbo].[User]]Audit] WITH NOCHECK ADD CONSTRAINT [CK_Disabled] CHECK ([Id] > 0);", postCreateScript.Script);
        Assert.Contains("ALTER TABLE [dbo].[User]]Audit] NOCHECK CONSTRAINT [CK_Disabled];", postCreateScript.Script);
        Assert.True(
            postCreateScript.Script.IndexOf("WITH NOCHECK ADD CONSTRAINT [CK_Disabled]", StringComparison.Ordinal) <
            postCreateScript.Script.IndexOf("NOCHECK CONSTRAINT [CK_Disabled]", StringComparison.Ordinal));
    }

    [Fact]
    public void BuildTableScripts_DoesNotEmitNotNullForNonPersistedComputedColumns()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "NameLength",
                Ordinal = 1,
                DataType = "int",
                IsNullable = false,
                ComputedDefinition = "(len([Name]))",
                IsPersisted = false
            }
        };

        SqlServerScriptInfo script = Assert.Single(SqlServerManagementScripting.BuildTableScripts(columns));

        Assert.Contains("[NameLength] AS (len([Name]))", script.Script);
        Assert.DoesNotContain("[NameLength] AS (len([Name])) NOT NULL", script.Script);
    }

    [Fact]
    public void TableScriptColumnQuery_PreservesMaskingAndTemporalHistoryMetadata()
    {
        string supportQuery = GetPrivateStaticString<SqlServer>("SqlServerMaskedColumnsSupportQuery");
        string template = GetPrivateStaticString<SqlServer>("SqlServerTableScriptColumnsManagementQuery");
        string maskingQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerTableScriptColumnsManagementQuery", true);
        string legacyQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerTableScriptColumnsManagementQuery", false);

        Assert.Contains("OBJECT_ID(N'sys.masked_columns')", supportQuery);
        Assert.Contains("MaskingFunction = {MaskingFunction}", template);
        Assert.Contains("LEFT JOIN sys.masked_columns AS masking_info", maskingQuery);
        Assert.Contains("MaskingFunction = masking_info.masking_function", maskingQuery);
        Assert.DoesNotContain("sys.masked_columns", legacyQuery);
        Assert.Contains("MaskingFunction = CONVERT(nvarchar(4000), NULL)", legacyQuery);
        Assert.Contains("/table/history_table_id", maskingQuery);
        Assert.Contains("LEFT JOIN sys.tables AS history_table ON history_table.object_id = temporal_info.history_table_id", maskingQuery);
        Assert.DoesNotContain("TableTemporalHistoryTableId", maskingQuery);
    }

    [Fact]
    public void BuildTableScripts_EmitsPostCreateScriptsAfterAllTables()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Child",
                ColumnName = "ParentId",
                Ordinal = 1,
                DataType = "int",
                IsNullable = false,
                PostCreateStatements = "ALTER TABLE [dbo].[Child] ADD CONSTRAINT [FK_Child_Parent] FOREIGN KEY ([ParentId]) REFERENCES [dbo].[Parent] ([Id]);"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Parent",
                ColumnName = "Id",
                Ordinal = 1,
                DataType = "int",
                IsNullable = false
            }
        };

        IReadOnlyList<SqlServerScriptInfo> scripts = SqlServerManagementScripting.BuildTableScripts(columns);

        Assert.Collection(
            scripts,
            script => Assert.Equal("Table", script.ScriptType),
            script => Assert.Equal("Table", script.ScriptType),
            script =>
            {
                Assert.Equal("TablePostCreate", script.ScriptType);
                Assert.Contains("ALTER TABLE [dbo].[Child] ADD CONSTRAINT [FK_Child_Parent]", script.Script);
            });
    }

    [Fact]
    public void BuildTableScripts_GeneratesLedgerColumnsAndOptions()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "LedgerAudit",
                ColumnName = "Id",
                Ordinal = 1,
                DataType = "int",
                IsNullable = false,
                LedgerType = 3
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "LedgerAudit",
                ColumnName = "LedgerTransactionIdStart",
                Ordinal = 2,
                DataType = "bigint",
                IsNullable = false,
                GeneratedAlwaysTypeDescription = "AS_TRANSACTION_ID_START",
                IsHidden = true,
                LedgerType = 3
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "LedgerAudit",
                ColumnName = "LedgerSequenceNumberStart",
                Ordinal = 3,
                DataType = "bigint",
                IsNullable = false,
                GeneratedAlwaysTypeDescription = "AS_SEQUENCE_NUMBER_START",
                IsHidden = true,
                LedgerType = 3
            }
        };

        SqlServerScriptInfo script = Assert.Single(SqlServerManagementScripting.BuildTableScripts(columns));

        Assert.Contains("[LedgerTransactionIdStart] bigint GENERATED ALWAYS AS TRANSACTION_ID START HIDDEN NOT NULL", script.Script);
        Assert.Contains("[LedgerSequenceNumberStart] bigint GENERATED ALWAYS AS SEQUENCE_NUMBER START HIDDEN NOT NULL", script.Script);
        Assert.Contains("WITH (LEDGER = ON (APPEND_ONLY = ON));", script.Script);
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
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "ValidFrom",
                Ordinal = 5,
                DataType = "datetime2(7)",
                GeneratedAlwaysTypeDescription = "AS_ROW_START"
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
        Assert.DoesNotContain(plan.Columns, column => column.SourceColumn == "ValidFrom");
        Assert.DoesNotContain("[SearchName]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[RowVersion]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[ValidFrom]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[SearchName]", plan.DestinationMergeCommand ?? string.Empty);
        Assert.DoesNotContain("[RowVersion]", plan.DestinationMergeCommand ?? string.Empty);
        Assert.DoesNotContain("[ValidFrom]", plan.DestinationMergeCommand ?? string.Empty);
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

    [Fact]
    public void BuildTableCopyPlan_DisablesMergeWhenKeyColumnIsNotWritable()
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
                ColumnName = "ComputedKey",
                Ordinal = 2,
                DataType = "int",
                ComputedDefinition = "[Id] + 1"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "DisplayName",
                Ordinal = 3,
                DataType = "nvarchar(128)"
            }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "Id", Ordinal = 1, IsPrimaryKey = true, IsUnique = true },
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "ComputedKey", Ordinal = 2, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Users",
            "archive",
            "Users",
            columns,
            indexes);

        Assert.Contains("Id", plan.KeyColumns);
        Assert.Contains("ComputedKey", plan.KeyColumns);
        Assert.Null(plan.DestinationMergeCommand);
        Assert.DoesNotContain(plan.Columns, column => column.SourceColumn == "ComputedKey");
    }

    [Fact]
    public void BuildTableCopyPlan_DoesNotUpdateNonKeyIdentityColumns()
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
                ColumnName = "ExternalId",
                Ordinal = 2,
                DataType = "uniqueidentifier"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "DisplayName",
                Ordinal = 3,
                DataType = "nvarchar(128)"
            }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "ExternalId", Ordinal = 1, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Users",
            "archive",
            "Users",
            columns,
            indexes);

        Assert.True(plan.RequiresIdentityInsert);
        Assert.Contains("ON target.[ExternalId] = source.[ExternalId]", plan.DestinationMergeCommand);
        Assert.Contains("UPDATE SET target.[DisplayName] = source.[DisplayName]", plan.DestinationMergeCommand);
        Assert.DoesNotContain("target.[Id] = source.[Id]", plan.DestinationMergeCommand);
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

    private static string GetPrivateStaticString<T>(string fieldName)
    {
        FieldInfo? field = typeof(T).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        return Assert.IsType<string>(field!.GetRawConstantValue());
    }

    private static string InvokePrivateStaticString<T>(string methodName, bool argument)
    {
        MethodInfo? method = typeof(T).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return Assert.IsType<string>(method!.Invoke(null, new object[] { argument }));
    }
}
