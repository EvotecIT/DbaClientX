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
    public void ManagementQueries_FilterFixedRolesAndResolvePermissionSecurables()
    {
        string serverPrincipals = GetPrivateStaticString<SqlServer>("SqlServerServerPrincipalsManagementQuery");
        string databasePrincipals = GetPrivateStaticString<SqlServer>("SqlServerDatabasePrincipalsManagementQuery");
        string roleMemberships = GetPrivateStaticString<SqlServer>("SqlServerRoleMembershipsManagementQuery");
        string availabilityGroupsSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerAvailabilityGroupsSupportQuery");
        string serverPermissionsSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerServerPermissionsSupportQuery");
        string fullTextStoplistsSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerFullTextStoplistsSupportQuery");
        string searchPropertyListsSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerSearchPropertyListsSupportQuery");
        string databaseScopedCredentialsSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerDatabaseScopedCredentialsSupportQuery");
        string externalLanguagesSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerExternalLanguagesSupportQuery");
        string permissions = InvokePrivateStaticString<SqlServer>("BuildSqlServerPermissionsManagementQuery", true, true, true, true, true, true);
        string partialAdvancedPermissions = InvokePrivateStaticString<SqlServer>("BuildSqlServerPermissionsManagementQuery", false, true, true, false, true, false);
        string legacyServerPermissions = InvokePrivateStaticString<SqlServer>("BuildSqlServerPermissionsManagementQuery", false, true, false, false, false, false);
        string legacyPermissions = InvokePrivateStaticString<SqlServer>("BuildSqlServerPermissionsManagementQuery", false, false, false, false, false, false);

        Assert.Contains("ISNULL(sp.is_fixed_role, 0) = 0", serverPrincipals);
        Assert.Contains("UNION ALL", serverPrincipals);
        Assert.Contains("FROM sys.sql_logins AS sl", serverPrincipals);
        Assert.Contains("WHERE NOT EXISTS", serverPrincipals);
        Assert.Contains("ORDER BY TypeDescription, Name", serverPrincipals);
        Assert.Contains("FROM sys.sql_logins AS sql_login", roleMemberships);
        Assert.Contains("WHERE server_principal.principal_id = sql_login.principal_id", roleMemberships);
        Assert.Contains("member_principal.principal_id = membership.member_principal_id", roleMemberships);
        Assert.Contains("ISNULL(dp.is_fixed_role, 0) = 0", databasePrincipals);
        Assert.Contains("OBJECT_ID(N'sys.availability_groups')", availabilityGroupsSupportQuery);
        Assert.Contains("COL_LENGTH(N'sys.availability_replicas', N'replica_metadata_id')", availabilityGroupsSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.server_permissions')", serverPermissionsSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.fulltext_stoplists')", fullTextStoplistsSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.registered_search_property_lists')", searchPropertyListsSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.database_scoped_credentials')", databaseScopedCredentialsSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.external_languages')", externalLanguagesSupportQuery);
        Assert.Contains("FROM sys.server_permissions AS permission", permissions);
        Assert.Contains("LEFT JOIN sys.certificates AS target_certificate", permissions);
        Assert.Contains("LEFT JOIN sys.assemblies AS target_assembly", permissions);
        Assert.Contains("LEFT JOIN sys.symmetric_keys AS target_symmetric_key", permissions);
        Assert.Contains("LEFT JOIN sys.xml_schema_collections AS target_xml_collection", permissions);
        Assert.Contains("LEFT JOIN sys.service_contracts AS target_service_contract", permissions);
        Assert.Contains("LEFT JOIN sys.services AS target_service", permissions);
        Assert.Contains("FROM sys.availability_groups AS availability_group", permissions);
        Assert.Contains("availability_replica.replica_metadata_id", permissions);
        Assert.Contains("WHEN N'AVAILABILITY GROUP' THEN COALESCE(availability_group.name", permissions);
        Assert.DoesNotContain("sys.availability_groups", legacyPermissions);
        Assert.DoesNotContain("FROM sys.server_permissions AS permission", legacyPermissions);
        Assert.Contains("WHEN N'AVAILABILITY GROUP' THEN CONVERT(nvarchar(128), permission.major_id)", legacyServerPermissions);
        Assert.Contains("WHEN permission.class_desc = N'CERTIFICATE' THEN target_certificate.name", permissions);
        Assert.Contains("WHEN permission.class_desc = N'SYMMETRIC_KEYS' THEN target_symmetric_key.name", permissions);
        Assert.Contains("permission.class_desc = N'SYMMETRIC_KEYS'", permissions);
        Assert.DoesNotContain("permission.class_desc = N'SYMMETRIC_KEY'", permissions);
        Assert.Contains("WHEN permission.class_desc = N'XML_SCHEMA_COLLECTION' THEN target_xml_collection.name", permissions);
        Assert.Contains("WHEN permission.class_desc = N'SERVICE_CONTRACT' THEN target_service_contract.name", permissions);
        Assert.Contains("WHEN permission.class_desc = N'FULLTEXT STOPLIST' THEN target_fulltext_stoplist.name", permissions);
        Assert.Contains("WHEN permission.class_desc = N'SEARCH PROPERTY LIST' THEN target_search_property_list.name", permissions);
        Assert.Contains("WHEN permission.class_desc = N'DATABASE SCOPED CREDENTIAL' THEN target_database_scoped_credential.name", permissions);
        Assert.Contains("WHEN permission.class_desc = N'EXTERNAL LANGUAGE' THEN target_external_language.language", permissions);
        Assert.DoesNotContain("permission.class_desc = N'FULLTEXT_STOPLIST'", permissions);
        Assert.DoesNotContain("permission.class_desc = N'SEARCH_PROPERTY_LIST'", permissions);
        Assert.DoesNotContain("permission.class_desc = N'DATABASE_SCOPED_CREDENTIAL'", permissions);
        Assert.DoesNotContain("permission.class_desc = N'EXTERNAL_LANGUAGE'", permissions);
        Assert.Contains("LEFT JOIN sys.external_languages AS target_external_language", permissions);
        Assert.Contains("target_fulltext_stoplist", partialAdvancedPermissions);
        Assert.Contains("target_database_scoped_credential", partialAdvancedPermissions);
        Assert.DoesNotContain("target_search_property_list", partialAdvancedPermissions);
        Assert.DoesNotContain("target_external_language", partialAdvancedPermissions);
        Assert.DoesNotContain("target_external_language", legacyPermissions);
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
        Assert.Contains("CREATE PROCEDURE [dbo].[DoWork]", script);
        Assert.Contains("ALTER TABLE [dbo].[T]", script);
    }

    [Fact]
    public void NormalizeModuleScript_RewritesAlterAfterLeadingComments()
    {
        string script = SqlServerManagementMappers.NormalizeModuleScript("-- generated by SQL Server\r\n/* body comment */\r\nALTER TRIGGER [DatabaseAudit] ON DATABASE FOR CREATE_TABLE AS SELECT 1;");

        Assert.Contains("-- generated by SQL Server", script);
        Assert.Contains("/* body comment */", script);
        Assert.Contains("CREATE TRIGGER [DatabaseAudit]", script);
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
                ColumnName = "SparseColumns",
                Ordinal = 6,
                DataType = "xml",
                IsNullable = true,
                IsColumnSet = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Blob",
                Ordinal = 7,
                DataType = "varbinary(max) FILESTREAM",
                IsNullable = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "SparseCode",
                Ordinal = 8,
                DataType = "int",
                IsNullable = true,
                IsSparse = true
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Email",
                Ordinal = 9,
                DataType = "nvarchar(256)",
                IsNullable = true,
                EncryptionDefinition = "ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [CEK_Audit], ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256')",
                MaskingFunction = "email()"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "NameLength",
                Ordinal = 10,
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
                Ordinal = 11,
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
                Ordinal = 12,
                DataType = "datetime2(7)",
                IsNullable = false,
                GeneratedAlwaysTypeDescription = "AS_ROW_END",
                IsHidden = true,
                TemporalType = 2,
                HistoryTableSchema = "history",
                HistoryTableName = "UserAuditHistory"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "MemoryCode",
                Ordinal = 13,
                DataType = "int",
                IsNullable = false,
                IsMemoryOptimized = true,
                DurabilityDescription = "SCHEMA_ONLY"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "User]Audit",
                ColumnName = "Embedding",
                Ordinal = 14,
                DataType = "vector(3, float16)",
                IsNullable = true
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
        Assert.Contains("[SparseColumns] xml COLUMN_SET FOR ALL_SPARSE_COLUMNS", script.Script);
        Assert.DoesNotContain("[SparseColumns] xml NULL", script.Script);
        Assert.Contains("[Blob] varbinary(max) FILESTREAM NULL", script.Script);
        Assert.Contains("[SparseCode] int SPARSE NULL", script.Script);
        Assert.Contains("[Email] nvarchar(256) ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [CEK_Audit], ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') MASKED WITH (FUNCTION = N'email()') NULL", script.Script);
        Assert.Contains("[NameLength] AS (len([Name])) PERSISTED NOT NULL", script.Script);
        Assert.Contains("[ValidFrom] datetime2(7) GENERATED ALWAYS AS ROW START HIDDEN NOT NULL", script.Script);
        Assert.Contains("[ValidTo] datetime2(7) GENERATED ALWAYS AS ROW END HIDDEN NOT NULL", script.Script);
        Assert.Contains("[MemoryCode] int NOT NULL", script.Script);
        Assert.Contains("[Embedding] vector(3, float16) NULL", script.Script);
        Assert.Contains("CONSTRAINT [PK_UserAudit] PRIMARY KEY CLUSTERED ([Id] ASC)", script.Script);
        Assert.Contains("CONSTRAINT [UQ_UserAudit_Name] UNIQUE NONCLUSTERED ([Name] ASC)", script.Script);
        Assert.Contains("CONSTRAINT [CK_UserAudit_Name] CHECK ([Name]\r\n <> N'')", script.Script);
        Assert.Contains("CONSTRAINT [CK_UserAudit_Code] CHECK ([Code] <> N'')", script.Script);
        Assert.Contains("CONSTRAINT [CK_UserAudit_code] CHECK ([code] <> N'')", script.Script);
        Assert.DoesNotContain("FOREIGN KEY", script.Script);
        Assert.Contains("PERIOD FOR SYSTEM_TIME ([ValidFrom], [ValidTo])", script.Script);
        Assert.Contains("WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [history].[UserAuditHistory]), MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_ONLY);", script.Script);
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
        string encryptionSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerColumnEncryptionSupportQuery");
        string graphEdgeSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerGraphEdgeConstraintsSupportQuery");
        string hashSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerHashIndexesSupportQuery");
        string fileTableSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerFileTablesSupportQuery");
        string serverTriggerSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerServerTriggerModulesSupportQuery");
        string databaseClrSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerDatabaseClrModulesSupportQuery");
        string databaseClrFunctionOrderingSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerDatabaseClrFunctionOrderingSupportQuery");
        string agentCatalogSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerAgentCatalogSupportQuery");
        string modulesTemplate = GetPrivateStaticString<SqlServer>("SqlServerModuleScriptsManagementQuery");
        string modulesQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerModuleScriptsManagementQuery", true, true, true);
        string noFunctionOrderQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerModuleScriptsManagementQuery", true, true, false);
        string noClrModulesQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerModuleScriptsManagementQuery", true, false, false);
        string legacyModulesQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerModuleScriptsManagementQuery", false, false, false);
        string template = GetPrivateStaticString<SqlServer>("SqlServerTableScriptColumnsManagementQuery");
        string modernQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerTableScriptColumnsManagementQuery", true, true, true, true, true, false, true);
        string legacyQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerTableScriptColumnsManagementQuery", false, false, false, false, false, false, true);
        string copyQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerTableScriptColumnsManagementQuery", false, false, false, false, false, true, false);

        Assert.Contains("OBJECT_ID(N'sys.masked_columns')", supportQuery);
        Assert.Contains("OBJECT_ID(N'sys.column_encryption_keys')", encryptionSupportQuery);
        Assert.Contains("COL_LENGTH(N'sys.columns', N'encryption_type_desc')", encryptionSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.edge_constraints')", graphEdgeSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.edge_constraint_clauses')", graphEdgeSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.hash_indexes')", hashSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.filetables')", fileTableSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.server_triggers')", serverTriggerSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.server_sql_modules')", serverTriggerSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.server_assembly_modules')", serverTriggerSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.assembly_modules')", databaseClrSupportQuery);
        Assert.DoesNotContain("OBJECT_ID(N'sys.function_order_columns')", databaseClrSupportQuery);
        Assert.Contains("OBJECT_ID(N'sys.function_order_columns')", databaseClrFunctionOrderingSupportQuery);
        Assert.Contains("DB_ID(N'msdb')", agentCatalogSupportQuery);
        Assert.Contains("OBJECT_ID(N'msdb.dbo.sysjobs')", agentCatalogSupportQuery);
        Assert.Contains("{ServerTriggerModuleScripts}", modulesTemplate);
        Assert.Contains("{DatabaseClrModuleScripts}", modulesTemplate);
        Assert.Contains("FROM sys.triggers AS trigger_info", modulesQuery);
        Assert.Contains("trigger_info.parent_class = 0", modulesQuery);
        Assert.Contains("INNER JOIN sys.assembly_modules AS assembly_module", modulesQuery);
        Assert.Contains("AS EXTERNAL NAME", modulesQuery);
        Assert.Contains("WHERE object_info.type IN ('PC', 'FS', 'FT', 'TA', 'AF')", modulesQuery);
        Assert.Contains("assembly_module.execute_as_principal_id", modulesQuery);
        Assert.Contains("RETURNS NULL ON NULL INPUT", modulesQuery);
        Assert.Contains("parameter_item.has_default_value", modulesQuery);
        Assert.Contains("parameter_item.default_value", modulesQuery);
        Assert.Contains("SQL_VARIANT_PROPERTY(parameter_item.default_value, N'BaseType')) IN (N'binary', N'varbinary')", modulesQuery);
        Assert.Contains("CONVERT(nvarchar(max), CONVERT(varbinary(max), parameter_item.default_value), 1)", modulesQuery);
        Assert.Contains("FROM sys.trigger_events AS event_info", modulesQuery);
        Assert.Contains("clr_trigger.is_instead_of_trigger", modulesQuery);
        Assert.Contains("N' INSTEAD OF '", modulesQuery);
        Assert.Contains("FROM sys.server_triggers AS trigger_info", modulesQuery);
        Assert.Contains("INNER JOIN sys.server_sql_modules AS module_info", modulesQuery);
        Assert.Contains("INNER JOIN sys.server_assembly_modules AS assembly_module", modulesQuery);
        Assert.Contains("FROM sys.server_trigger_events AS event_info", modulesQuery);
        Assert.Contains("INNER JOIN master.sys.assemblies AS assembly_info", modulesQuery);
        Assert.Contains("LEFT JOIN sys.server_principals AS execute_as_principal", modulesQuery);
        Assert.Contains("assembly_module.execute_as_principal_id", modulesQuery);
        Assert.Contains("server_trigger_options.OptionClause", modulesQuery);
        Assert.Contains("ObjectName = trigger_info.name COLLATE DATABASE_DEFAULT", modulesQuery);
        Assert.Contains("ObjectType = trigger_info.type_desc COLLATE DATABASE_DEFAULT", modulesQuery);
        Assert.Contains("module_info.definition COLLATE DATABASE_DEFAULT", modulesQuery);
        Assert.Contains("AS EXTERNAL NAME", modulesQuery);
        Assert.Contains("N' ON ALL SERVER', server_trigger_options.OptionClause, N' FOR '", modulesQuery);
        Assert.Contains("N'CREATE TRIGGER ', QUOTENAME(trigger_info.name)", modulesQuery);
        Assert.Contains("N' ON DATABASE', database_trigger_options.OptionClause", modulesQuery);
        Assert.Contains("INNER JOIN sys.assembly_modules AS assembly_module ON assembly_module.object_id = trigger_info.object_id", modulesQuery);
        Assert.Contains("clr_trigger.is_not_for_replication", modulesQuery);
        Assert.Contains("object_info.type = N'TA' AND clr_trigger.is_disabled = 1", modulesQuery);
        Assert.DoesNotContain("sys.server_triggers", legacyModulesQuery);
        Assert.DoesNotContain("sys.server_sql_modules", legacyModulesQuery);
        Assert.DoesNotContain("sys.server_assembly_modules", legacyModulesQuery);
        Assert.DoesNotContain("sys.assembly_modules", legacyModulesQuery);
        Assert.Contains("object_info.type = N'AF'", modulesQuery);
        Assert.Contains("N'CREATE AGGREGATE '", modulesQuery);
        Assert.Contains("N'EXTERNAL NAME ',", modulesQuery);
        Assert.Contains("QUOTENAME(assembly_info.name), N'.', QUOTENAME(assembly_module.assembly_class))", modulesQuery);
        Assert.DoesNotContain("N'CREATE AGGREGATE '", noClrModulesQuery);
        Assert.DoesNotContain("sys.assembly_modules", noClrModulesQuery);
        Assert.Contains("object_info.type IN ('PC', 'FS', 'FT', 'TA', 'AF')", modulesQuery);
        Assert.Contains("FROM sys.function_order_columns AS function_order_column", modulesQuery);
        Assert.Contains("function_order_info.OrderClause", modulesQuery);
        Assert.Contains("N'RETURNS TABLE (' + COALESCE(table_return_info.TableDefinition, N'') + N')' + clr_options.OptionClause + function_order_info.OrderClause", modulesQuery);
        Assert.Contains("parameter_item.xml_collection_id <> 0", modulesQuery);
        Assert.Contains("N'xml(' + CASE WHEN parameter_item.is_xml_document = 1 THEN N'DOCUMENT ' ELSE N'CONTENT ' END", modulesQuery);
        Assert.Contains("column_item.xml_collection_id <> 0", modulesQuery);
        Assert.Contains("N'GO' + CHAR(13) + CHAR(10) + N'DISABLE TRIGGER", modulesQuery);
        Assert.Contains("DISABLE TRIGGER ' + QUOTENAME(schema_info.name) + N'.' + QUOTENAME(object_info.name)", modulesQuery);
        Assert.Contains("DISABLE TRIGGER ' + QUOTENAME(trigger_info.name) + N' ON DATABASE", modulesQuery);
        Assert.Contains("DISABLE TRIGGER ' + QUOTENAME(trigger_info.name COLLATE DATABASE_DEFAULT) + N' ON ALL SERVER", modulesQuery);
        Assert.Contains("sys.sp_settriggerorder", modulesQuery);
        Assert.Contains("event_info.is_first = 1 OR event_info.is_last = 1", modulesQuery);
        Assert.Contains("@namespace = N''DATABASE''", modulesQuery);
        Assert.Contains("@namespace = N''SERVER''", modulesQuery);
        Assert.Contains("ORDER BY SchemaName, ObjectName", modulesQuery);
        Assert.Contains("sys.assembly_modules", noFunctionOrderQuery);
        Assert.DoesNotContain("sys.function_order_columns", noFunctionOrderQuery);
        Assert.DoesNotContain("function_order_info.OrderClause", noFunctionOrderQuery);
        Assert.Contains("MaskingFunction = {MaskingFunction}", template);
        Assert.Contains("EncryptionDefinition = {EncryptionDefinition}", template);
        Assert.Contains("{GraphEdgeConstraintStatements}", template);
        Assert.Contains("{GraphTableOnlyRows}", template);
        Assert.Contains("{GraphHiddenColumnFilter}", template);
        Assert.Contains("{FileTableOptions}", template);
        Assert.Contains("{FileTableJoin}", template);
        Assert.Contains("LEFT JOIN sys.masked_columns AS masking_info", modernQuery);
        Assert.Contains("MaskingFunction = masking_info.masking_function", modernQuery);
        Assert.Contains("LEFT JOIN sys.column_encryption_keys AS encryption_key", modernQuery);
        Assert.Contains("ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ", modernQuery);
        Assert.Contains("FROM sys.filetables AS filetable_source", modernQuery);
        Assert.Contains("FileTableOptions = filetable_info.options", modernQuery);
        Assert.Contains("FILETABLE_DIRECTORY", modernQuery);
        Assert.Contains("FILETABLE_PRIMARY_KEY_CONSTRAINT_NAME", modernQuery);
        Assert.DoesNotContain("sys.masked_columns", legacyQuery);
        Assert.DoesNotContain("sys.column_encryption_keys", legacyQuery);
        Assert.DoesNotContain("sys.filetables", legacyQuery);
        Assert.Contains("MaskingFunction = CONVERT(nvarchar(4000), NULL)", legacyQuery);
        Assert.Contains("EncryptionDefinition = CONVERT(nvarchar(4000), NULL)", legacyQuery);
        Assert.Contains("FileTableOptions = CONVERT(nvarchar(max), NULL)", legacyQuery);
        Assert.Contains("/table/history_table_id", modernQuery);
        Assert.Contains("/table/history_retention_period", modernQuery);
        Assert.Contains("/table/history_retention_period_unit_desc", modernQuery);
        Assert.Contains("HistoryRetentionPeriod = temporal_info.history_retention_period", modernQuery);
        Assert.Contains("HistoryRetentionPeriodUnit = temporal_info.history_retention_period_unit_desc", modernQuery);
        Assert.Contains("/table/is_memory_optimized", modernQuery);
        Assert.Contains("/table/durability_desc", modernQuery);
        Assert.Contains("LEFT JOIN sys.hash_indexes AS primary_key_hash", modernQuery);
        Assert.Contains("PrimaryKeyBucketCount = primary_key_hash.bucket_count", modernQuery);
        Assert.DoesNotContain("sys.hash_indexes", legacyQuery);
        Assert.Contains("PrimaryKeyBucketCount = CONVERT(bigint, NULL)", legacyQuery);
        Assert.Contains("IsColumnSet = CONVERT(bit, column_info.is_column_set)", modernQuery);
        Assert.Contains("GraphColumnRole = graph_column_info.graph_column_role", modernQuery);
        Assert.Contains("/column/graph_type", modernQuery);
        Assert.Contains("GraphTableKind = graph_info.graph_kind", modernQuery);
        Assert.Contains("/table/is_filetable", modernQuery);
        Assert.Contains("/table/is_node", modernQuery);
        Assert.Contains("/table/is_edge", modernQuery);
        Assert.Contains("/table/is_external", modernQuery);
        Assert.Contains("external_info.is_external = 0", modernQuery);
        Assert.Contains("LEFT JOIN sys.hash_indexes AS unique_hash", modernQuery);
        Assert.Contains("LEFT JOIN sys.hash_indexes AS memory_hash", modernQuery);
        Assert.Contains("INDEX ' + QUOTENAME(memory_index.name)", modernQuery);
        Assert.Contains("FROM sys.edge_constraints AS edge_constraint", modernQuery);
        Assert.Contains("FROM sys.edge_constraint_clauses AS edge_clause", modernQuery);
        Assert.DoesNotContain("sys.edge_constraints", legacyQuery);
        Assert.DoesNotContain("sys.edge_constraint_clauses", legacyQuery);
        Assert.Contains("ledger_info.ledger_type <> 1", modernQuery);
        Assert.Contains("/table/ledger_view_id", modernQuery);
        Assert.Contains("LEFT JOIN sys.views AS ledger_view ON ledger_view.object_id = ledger_info.ledger_view_id", modernQuery);
        Assert.Contains("LedgerViewName = ledger_view.name", modernQuery);
        Assert.Contains("LedgerTransactionIdColumnName = ledger_view_column_info.transaction_id_column_name", modernQuery);
        Assert.Contains("/column/vector_dimensions", modernQuery);
        Assert.Contains("/column/vector_base_type_desc", modernQuery);
        Assert.Contains("type_info.name = N'vector'", modernQuery);
        Assert.Contains("N'vector('", modernQuery);
        Assert.Contains("edge_constraint.delete_referential_action_desc", modernQuery);
        Assert.Contains("N' ON DELETE ' + REPLACE(edge_constraint.delete_referential_action_desc", modernQuery);
        Assert.Contains("edge_constraint.is_not_trusted = 1 OR edge_constraint.is_disabled = 1", modernQuery);
        Assert.Contains("N' WITH NOCHECK' ELSE N' WITH CHECK' END +", modernQuery);
        Assert.Contains("ColumnName = CONVERT(sysname, N'')", modernQuery);
        Assert.Contains("PostCreateStatements = graph_only_post_create_info.statements", modernQuery);
        Assert.DoesNotContain("{GraphEdgeConstraintStatements}", modernQuery);
        Assert.DoesNotContain("ColumnName = CONVERT(sysname, N'')", copyQuery);
        Assert.Contains("NOT (graph_info.graph_kind IN (N'NODE', N'EDGE') AND graph_column_info.graph_type IS NOT NULL)", modernQuery);
        Assert.Contains("graph_column_info.graph_type NOT IN (5, 8)", copyQuery);
        Assert.DoesNotContain("column_info.name NOT IN (N'$from_id', N'$to_id')", copyQuery);
        Assert.Contains("LEFT JOIN sys.tables AS history_table ON history_table.object_id = temporal_info.history_table_id", modernQuery);
        Assert.DoesNotContain("TableTemporalHistoryTableId", modernQuery);
        Assert.Contains("ORDER BY SchemaName, TableName, Ordinal", modernQuery);
    }

    [Fact]
    public void DependencyQuery_IncludesDdlTriggerDependencies()
    {
        string serverTriggerSupportQuery = GetPrivateStaticString<SqlServer>("SqlServerServerTriggersSupportQuery");
        string dependenciesTemplate = GetPrivateStaticString<SqlServer>("SqlServerDependenciesManagementQuery");
        string dependenciesQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerDependenciesManagementQuery", true);
        string legacyDependenciesQuery = InvokePrivateStaticString<SqlServer>("BuildSqlServerDependenciesManagementQuery", false);

        Assert.Contains("OBJECT_ID(N'sys.server_triggers')", serverTriggerSupportQuery);
        Assert.Contains("{ServerTriggerDependencies}", dependenciesTemplate);
        Assert.Contains("dependency.referencing_class = 1", dependenciesQuery);
        Assert.Contains("INNER JOIN sys.triggers AS trigger_info ON trigger_info.object_id = dependency.referencing_id", dependenciesQuery);
        Assert.Contains("dependency.referencing_class = 12", dependenciesQuery);
        Assert.Contains("trigger_info.parent_class = 0", dependenciesQuery);
        Assert.Contains("FROM master.sys.sql_expression_dependencies AS dependency", dependenciesQuery);
        Assert.Contains("INNER JOIN master.sys.server_triggers AS trigger_info ON trigger_info.object_id = dependency.referencing_id", dependenciesQuery);
        Assert.Contains("dependency.referencing_class = 13", dependenciesQuery);
        Assert.Contains("ReferencingName = trigger_info.name COLLATE DATABASE_DEFAULT", dependenciesQuery);
        Assert.Contains("ReferencedClassDescription = dependency.referenced_class_desc COLLATE DATABASE_DEFAULT", dependenciesQuery);
        Assert.DoesNotContain("sys.server_triggers", legacyDependenciesQuery);
        Assert.DoesNotContain("dependency.referencing_class = 13", legacyDependenciesQuery);
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
                LedgerType = 3,
                LedgerViewSchema = "audit",
                LedgerViewName = "LedgerAuditView",
                LedgerTransactionIdColumnName = "TxnId",
                LedgerSequenceNumberColumnName = "SeqNo",
                LedgerOperationTypeColumnName = "OperationId",
                LedgerOperationTypeDescriptionColumnName = "OperationDescription"
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
        Assert.Contains("WITH (LEDGER = ON (LEDGER_VIEW = [audit].[LedgerAuditView] (TRANSACTION_ID_COLUMN_NAME = [TxnId], SEQUENCE_NUMBER_COLUMN_NAME = [SeqNo], OPERATION_TYPE_COLUMN_NAME = [OperationId], OPERATION_TYPE_DESC_COLUMN_NAME = [OperationDescription]), APPEND_ONLY = ON));", script.Script);
    }

    [Fact]
    public void BuildTableScripts_GeneratesMemoryOptimizedHashPrimaryKeyAndGraphOptions()
    {
        var hashColumns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "MemoryUsers",
                ColumnName = "Id",
                Ordinal = 1,
                DataType = "int",
                IsNullable = false,
                IsMemoryOptimized = true,
                DurabilityDescription = "SCHEMA_AND_DATA",
                PrimaryKeyName = "PK_MemoryUsers",
                PrimaryKeyOrdinal = 1,
                PrimaryKeyIndexType = "NONCLUSTERED HASH",
                PrimaryKeyBucketCount = 1024
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "MemoryUsers",
                ColumnName = "ExternalId",
                Ordinal = 2,
                DataType = "uniqueidentifier",
                IsNullable = false,
                IsMemoryOptimized = true,
                DurabilityDescription = "SCHEMA_AND_DATA",
                UniqueConstraintName = "UQ_MemoryUsers_ExternalId",
                UniqueConstraintOrdinal = 1,
                UniqueConstraintIndexType = "NONCLUSTERED HASH",
                UniqueConstraintBucketCount = 2048,
                AdditionalConstraintDefinitions = "INDEX [IX_MemoryUsers_ExternalId] NONCLUSTERED HASH ([ExternalId]) WITH (BUCKET_COUNT = 4096)"
            }
        };
        var graphColumns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "UserNode",
                ColumnName = "Name",
                Ordinal = 1,
                DataType = "nvarchar(128)",
                IsNullable = false,
                GraphTableKind = "NODE"
            }
        };
        var edgeColumns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "FriendEdge",
                ColumnName = "Weight",
                Ordinal = 1,
                DataType = "int",
                IsNullable = true,
                GraphTableKind = "EDGE",
                PostCreateStatements = "ALTER TABLE [dbo].[FriendEdge] ADD CONSTRAINT [EC_FriendEdge] CONNECTION ([dbo].[Person] TO [dbo].[Person]);"
            }
        };
        var graphOnlyColumns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Likes",
                ColumnName = "",
                Ordinal = 0,
                GraphTableKind = "EDGE",
                PostCreateStatements = "ALTER TABLE [dbo].[Likes] ADD CONSTRAINT [EC_Likes] CONNECTION ([dbo].[Person] TO [dbo].[Post]);"
            }
        };
        var fileTableColumns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Documents",
                ColumnName = "stream_id",
                Ordinal = 1,
                DataType = "uniqueidentifier",
                IsNullable = false,
                GraphTableKind = "FILETABLE",
                FileTableOptions = "WITH (FILETABLE_DIRECTORY = N'Docs', FILETABLE_COLLATE_FILENAME = Latin1_General_CI_AS, FILETABLE_PRIMARY_KEY_CONSTRAINT_NAME = [PK_Documents])"
            }
        };

        SqlServerScriptInfo hashScript = Assert.Single(SqlServerManagementScripting.BuildTableScripts(hashColumns), item => item.ScriptType == "Table");
        SqlServerScriptInfo graphScript = Assert.Single(SqlServerManagementScripting.BuildTableScripts(graphColumns));
        IReadOnlyList<SqlServerScriptInfo> edgeScripts = SqlServerManagementScripting.BuildTableScripts(edgeColumns);
        SqlServerScriptInfo edgeScript = Assert.Single(edgeScripts, item => item.ScriptType == "Table");
        SqlServerScriptInfo edgePostCreateScript = Assert.Single(edgeScripts, item => item.ScriptType == "TablePostCreate");
        IReadOnlyList<SqlServerScriptInfo> graphOnlyScripts = SqlServerManagementScripting.BuildTableScripts(graphOnlyColumns);
        SqlServerScriptInfo graphOnlyScript = Assert.Single(graphOnlyScripts, item => item.ScriptType == "Table");
        SqlServerScriptInfo graphOnlyPostCreateScript = Assert.Single(graphOnlyScripts, item => item.ScriptType == "TablePostCreate");
        SqlServerScriptInfo fileTableScript = Assert.Single(SqlServerManagementScripting.BuildTableScripts(fileTableColumns));

        Assert.Contains("CONSTRAINT [PK_MemoryUsers] PRIMARY KEY NONCLUSTERED HASH ([Id]) WITH (BUCKET_COUNT = 1024)", hashScript.Script);
        Assert.Contains("CONSTRAINT [UQ_MemoryUsers_ExternalId] UNIQUE NONCLUSTERED HASH ([ExternalId]) WITH (BUCKET_COUNT = 2048)", hashScript.Script);
        Assert.Contains("INDEX [IX_MemoryUsers_ExternalId] NONCLUSTERED HASH ([ExternalId]) WITH (BUCKET_COUNT = 4096)", hashScript.Script);
        Assert.Contains("WITH (MEMORY_OPTIMIZED = ON, DURABILITY = SCHEMA_AND_DATA);", hashScript.Script);
        Assert.Contains("AS NODE;", graphScript.Script);
        Assert.Contains("AS EDGE;", edgeScript.Script);
        Assert.Contains("ALTER TABLE [dbo].[FriendEdge] ADD CONSTRAINT [EC_FriendEdge] CONNECTION ([dbo].[Person] TO [dbo].[Person]);", edgePostCreateScript.Script);
        Assert.Equal("CREATE TABLE [dbo].[Likes]" + Environment.NewLine + "AS EDGE;", graphOnlyScript.Script);
        Assert.Contains("ALTER TABLE [dbo].[Likes] ADD CONSTRAINT [EC_Likes] CONNECTION ([dbo].[Person] TO [dbo].[Post]);", graphOnlyPostCreateScript.Script);
        Assert.Equal(
            "CREATE TABLE [dbo].[Documents]" + Environment.NewLine +
            "AS FILETABLE" + Environment.NewLine +
            "WITH (FILETABLE_DIRECTORY = N'Docs', FILETABLE_COLLATE_FILENAME = Latin1_General_CI_AS, FILETABLE_PRIMARY_KEY_CONSTRAINT_NAME = [PK_Documents]);",
            fileTableScript.Script);
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
    public void BuildTableCopyPlan_MatchesKeyColumnsCaseSensitively()
    {
        var columns = new[]
        {
            new DbaColumnInfo("dbo", "Users", "Code", "int") { Ordinal = 1, IsNullable = false },
            new DbaColumnInfo("dbo", "Users", "code", "int") { Ordinal = 2, IsNullable = true },
            new DbaColumnInfo("dbo", "Users", "DisplayName", "nvarchar(128)") { Ordinal = 3, IsNullable = true }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Users", "PK_Users") { Column = "Code", Ordinal = 1, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Users",
            "archive",
            "Users",
            columns,
            indexes);

        Assert.Equal("Code", Assert.Single(plan.KeyColumns));
        Assert.True(Assert.Single(plan.Columns, column => column.SourceColumn == "Code").IsKey);
        Assert.False(Assert.Single(plan.Columns, column => column.SourceColumn == "code").IsKey);
        Assert.Contains("ON target.[Code] = source.[Code]", plan.DestinationMergeCommand);
        Assert.DoesNotContain("target.[code] = source.[code] AND", plan.DestinationMergeCommand);
        Assert.Contains("UPDATE SET target.[code] = source.[code], target.[DisplayName] = source.[DisplayName]", plan.DestinationMergeCommand);
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
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "SparseColumns",
                Ordinal = 6,
                DataType = "xml",
                IsColumnSet = true
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
        Assert.DoesNotContain(plan.Columns, column => column.SourceColumn == "SparseColumns");
        Assert.DoesNotContain("[SearchName]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[RowVersion]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[ValidFrom]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[SparseColumns]", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[SearchName]", plan.DestinationMergeCommand ?? string.Empty);
        Assert.DoesNotContain("[RowVersion]", plan.DestinationMergeCommand ?? string.Empty);
        Assert.DoesNotContain("[ValidFrom]", plan.DestinationMergeCommand ?? string.Empty);
        Assert.DoesNotContain("[SparseColumns]", plan.DestinationMergeCommand ?? string.Empty);
    }

    [Fact]
    public void BuildTableCopyPlan_IncludesGraphEdgeEndpointColumns()
    {
        var columns = new[]
        {
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Likes",
                ColumnName = "$edge_id",
                Ordinal = 1,
                DataType = "varbinary(1000)",
                IsNullable = false,
                IsHidden = true,
                GraphTableKind = "EDGE",
                GraphColumnRole = "$edge_id"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Likes",
                ColumnName = "$from_id",
                Ordinal = 2,
                DataType = "varbinary(1000)",
                IsNullable = false,
                IsHidden = true,
                GraphTableKind = "EDGE",
                GraphColumnRole = "$from_id"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Likes",
                ColumnName = "$to_id",
                Ordinal = 3,
                DataType = "varbinary(1000)",
                IsNullable = false,
                IsHidden = true,
                GraphTableKind = "EDGE",
                GraphColumnRole = "$to_id"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Likes",
                ColumnName = "Weight",
                Ordinal = 4,
                DataType = "int",
                GraphTableKind = "EDGE"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Likes",
                ColumnName = "Label",
                Ordinal = 5,
                DataType = "nvarchar(50)",
                GraphTableKind = "EDGE"
            }
        };
        var indexes = new[]
        {
            new DbaIndexInfo("dbo", "Likes", "PK_Likes") { Column = "Weight", Ordinal = 1, IsPrimaryKey = true, IsUnique = true }
        };

        SqlServerTableCopyPlan plan = SqlServerManagementScripting.BuildTableCopyPlan(
            "dbo",
            "Likes",
            "archive",
            "Likes",
            columns,
            indexes);

        Assert.Equal("SELECT [$from_id], [$to_id], [Weight], [Label] FROM [dbo].[Likes];", plan.SourceSelectCommand);
        Assert.Contains("INSERT INTO [archive].[Likes] ([$from_id], [$to_id], [Weight], [Label]) VALUES (@p0, @p1, @p2, @p3);", plan.DestinationInsertCommand);
        Assert.DoesNotContain("[$edge_id]", plan.SourceSelectCommand);
        Assert.NotNull(plan.DestinationMergeCommand);
        Assert.Contains("ON target.[Weight] = source.[Weight]", plan.DestinationMergeCommand);
        Assert.Contains("UPDATE SET target.[Label] = source.[Label]", plan.DestinationMergeCommand);
        Assert.DoesNotContain("target.[$from_id] = source.[$from_id]", plan.DestinationMergeCommand);
        Assert.DoesNotContain("target.[$to_id] = source.[$to_id]", plan.DestinationMergeCommand);
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
    public void BuildTableCopyPlan_MatchesIdentityColumnsCaseSensitively()
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
                ColumnName = "id",
                Ordinal = 2,
                DataType = "int"
            },
            new SqlServerTableColumnScriptInfo
            {
                SchemaName = "dbo",
                TableName = "Users",
                ColumnName = "ExternalId",
                Ordinal = 3,
                DataType = "uniqueidentifier"
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

        Assert.True(Assert.Single(plan.Columns, column => column.SourceColumn == "Id").IsIdentity);
        Assert.False(Assert.Single(plan.Columns, column => column.SourceColumn == "id").IsIdentity);
        Assert.Contains("UPDATE SET target.[id] = source.[id]", plan.DestinationMergeCommand);
        Assert.DoesNotContain("target.[Id] = source.[Id]", plan.DestinationMergeCommand);
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

    private static string InvokePrivateStaticString<T>(string methodName, params bool[] arguments)
    {
        MethodInfo? method = typeof(T).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);
        return Assert.IsType<string>(method!.Invoke(null, arguments.Cast<object>().ToArray()));
    }
}
