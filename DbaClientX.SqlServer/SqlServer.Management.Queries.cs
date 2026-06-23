namespace DBAClientX;

public partial class SqlServer
{
    private const string SqlServerAgentJobsManagementQuery = @"
SELECT
    JobId = j.job_id,
    Name = j.name,
    Category = c.name,
    OwnerLoginName = SUSER_SNAME(j.owner_sid),
    Description = j.description,
    Enabled = CONVERT(bit, j.enabled),
    Created = j.date_created,
    Modified = j.date_modified
FROM msdb.dbo.sysjobs AS j
LEFT JOIN msdb.dbo.syscategories AS c ON c.category_id = j.category_id
WHERE (@includeDisabled = 1 OR j.enabled = 1)
  AND (@jobName IS NULL OR j.name = @jobName)
ORDER BY j.name;";

    private const string SqlServerAgentJobStepsManagementQuery = @"
SELECT
    JobId = j.job_id,
    JobName = j.name,
    StepId = s.step_id,
    StepName = s.step_name,
    Subsystem = s.subsystem,
    Command = s.command,
    DatabaseName = s.database_name,
    OnSuccessAction = CASE s.on_success_action
        WHEN 1 THEN N'QuitWithSuccess'
        WHEN 2 THEN N'QuitWithFailure'
        WHEN 3 THEN N'GoToNextStep'
        WHEN 4 THEN N'GoToStep'
        ELSE CONVERT(nvarchar(32), s.on_success_action)
    END,
    OnFailAction = CASE s.on_fail_action
        WHEN 1 THEN N'QuitWithSuccess'
        WHEN 2 THEN N'QuitWithFailure'
        WHEN 3 THEN N'GoToNextStep'
        WHEN 4 THEN N'GoToStep'
        ELSE CONVERT(nvarchar(32), s.on_fail_action)
    END,
    RetryAttempts = s.retry_attempts,
    RetryInterval = s.retry_interval
FROM msdb.dbo.sysjobsteps AS s
INNER JOIN msdb.dbo.sysjobs AS j ON j.job_id = s.job_id
WHERE (@jobName IS NULL OR j.name = @jobName)
ORDER BY j.name, s.step_id;";

    private const string SqlServerAgentSchedulesManagementQuery = @"
SELECT
    JobId = j.job_id,
    JobName = j.name,
    ScheduleId = s.schedule_id,
    Name = s.name,
    Enabled = CONVERT(bit, s.enabled),
    FrequencyType = s.freq_type,
    FrequencyInterval = s.freq_interval,
    FrequencySubdayType = s.freq_subday_type,
    FrequencySubdayInterval = s.freq_subday_interval,
    ActiveStartDate = s.active_start_date,
    ActiveEndDate = s.active_end_date,
    ActiveStartTime = s.active_start_time,
    ActiveEndTime = s.active_end_time
FROM msdb.dbo.sysschedules AS s
LEFT JOIN msdb.dbo.sysjobschedules AS js ON js.schedule_id = s.schedule_id
LEFT JOIN msdb.dbo.sysjobs AS j ON j.job_id = js.job_id
WHERE (@includeDisabled = 1 OR s.enabled = 1)
  AND (@jobName IS NULL OR j.name = @jobName)
ORDER BY ISNULL(j.name, N''), s.name;";

    private const string SqlServerServerPrincipalsManagementQuery = @"
SELECT
    Scope = N'Server',
    DatabaseName = CONVERT(nvarchar(128), NULL),
    Name = sp.name,
    Type = sp.type,
    TypeDescription = sp.type_desc,
    Sid = CONVERT(varchar(514), sp.sid, 1),
    DefaultDatabaseName = sp.default_database_name,
    DefaultSchemaName = CONVERT(nvarchar(128), NULL),
    AuthenticationType = CONVERT(nvarchar(60), NULL),
    IsDisabled = CONVERT(bit, sl.is_disabled),
    IsFixedRole = CONVERT(bit, sp.is_fixed_role),
    Created = sp.create_date,
    Modified = sp.modify_date
FROM sys.server_principals AS sp
LEFT JOIN sys.sql_logins AS sl ON sl.principal_id = sp.principal_id
WHERE (@includeSystem = 1 OR sp.name NOT LIKE N'##MS_%##')
  AND (@name IS NULL OR sp.name COLLATE DATABASE_DEFAULT = @name)
ORDER BY sp.type_desc, sp.name;";

    private const string SqlServerDatabasePrincipalsManagementQuery = @"
SELECT
    Scope = N'Database',
    DatabaseName = DB_NAME(),
    Name = dp.name,
    Type = dp.type,
    TypeDescription = dp.type_desc,
    Sid = CONVERT(varchar(514), dp.sid, 1),
    DefaultDatabaseName = CONVERT(nvarchar(128), NULL),
    DefaultSchemaName = dp.default_schema_name,
    AuthenticationType = dp.authentication_type_desc,
    IsDisabled = CONVERT(bit, NULL),
    IsFixedRole = CONVERT(bit, dp.is_fixed_role),
    Created = dp.create_date,
    Modified = dp.modify_date
FROM sys.database_principals AS dp
WHERE (@includeSystem = 1 OR dp.name NOT IN (N'dbo', N'guest', N'INFORMATION_SCHEMA', N'sys'))
  AND (@name IS NULL OR dp.name = @name)
ORDER BY dp.type_desc, dp.name;";

    private const string SqlServerRoleMembershipsManagementQuery = @"
SELECT
    Scope = N'Server' COLLATE DATABASE_DEFAULT,
    DatabaseName = CONVERT(nvarchar(128), NULL),
    RoleName = role_principal.name COLLATE DATABASE_DEFAULT,
    RoleTypeDescription = role_principal.type_desc COLLATE DATABASE_DEFAULT,
    MemberName = member_principal.name COLLATE DATABASE_DEFAULT,
    MemberTypeDescription = member_principal.type_desc COLLATE DATABASE_DEFAULT
FROM sys.server_role_members AS membership
INNER JOIN sys.server_principals AS role_principal ON role_principal.principal_id = membership.role_principal_id
INNER JOIN sys.server_principals AS member_principal ON member_principal.principal_id = membership.member_principal_id
WHERE (@roleName IS NULL OR role_principal.name COLLATE DATABASE_DEFAULT = @roleName)
  AND (@memberName IS NULL OR member_principal.name COLLATE DATABASE_DEFAULT = @memberName)
UNION ALL
SELECT
    Scope = N'Database' COLLATE DATABASE_DEFAULT,
    DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT,
    RoleName = role_principal.name COLLATE DATABASE_DEFAULT,
    RoleTypeDescription = role_principal.type_desc COLLATE DATABASE_DEFAULT,
    MemberName = member_principal.name COLLATE DATABASE_DEFAULT,
    MemberTypeDescription = member_principal.type_desc COLLATE DATABASE_DEFAULT
FROM sys.database_role_members AS membership
INNER JOIN sys.database_principals AS role_principal ON role_principal.principal_id = membership.role_principal_id
INNER JOIN sys.database_principals AS member_principal ON member_principal.principal_id = membership.member_principal_id
WHERE (@roleName IS NULL OR role_principal.name = @roleName)
  AND (@memberName IS NULL OR member_principal.name = @memberName)
ORDER BY Scope, DatabaseName, RoleName, MemberName;";

    private const string SqlServerPermissionsManagementQuery = @"
SELECT
    Scope = N'Server' COLLATE DATABASE_DEFAULT,
    DatabaseName = CONVERT(nvarchar(128), NULL),
    State = permission.state COLLATE DATABASE_DEFAULT,
    StateDescription = permission.state_desc COLLATE DATABASE_DEFAULT,
    PermissionName = permission.permission_name COLLATE DATABASE_DEFAULT,
    ClassDescription = permission.class_desc COLLATE DATABASE_DEFAULT,
    SecurableSchema = CONVERT(nvarchar(128), NULL),
    SecurableName = CASE permission.class_desc
        WHEN N'SERVER' THEN CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName')) COLLATE DATABASE_DEFAULT
        WHEN N'ENDPOINT' THEN endpoint.name COLLATE DATABASE_DEFAULT
        WHEN N'SERVER_PRINCIPAL' THEN target_principal.name COLLATE DATABASE_DEFAULT
        ELSE CONVERT(nvarchar(256), permission.major_id)
    END,
    SecurableColumn = CONVERT(nvarchar(128), NULL),
    GranteeName = grantee.name COLLATE DATABASE_DEFAULT,
    GrantorName = grantor.name COLLATE DATABASE_DEFAULT
FROM sys.server_permissions AS permission
INNER JOIN sys.server_principals AS grantee ON grantee.principal_id = permission.grantee_principal_id
INNER JOIN sys.server_principals AS grantor ON grantor.principal_id = permission.grantor_principal_id
LEFT JOIN sys.endpoints AS endpoint ON endpoint.endpoint_id = permission.major_id AND permission.class_desc = N'ENDPOINT'
LEFT JOIN sys.server_principals AS target_principal ON target_principal.principal_id = permission.major_id AND permission.class_desc = N'SERVER_PRINCIPAL'
WHERE (@principalName IS NULL OR grantee.name COLLATE DATABASE_DEFAULT = @principalName)
UNION ALL
SELECT
    Scope = N'Database' COLLATE DATABASE_DEFAULT,
    DatabaseName = DB_NAME() COLLATE DATABASE_DEFAULT,
    State = permission.state COLLATE DATABASE_DEFAULT,
    StateDescription = permission.state_desc COLLATE DATABASE_DEFAULT,
    PermissionName = permission.permission_name COLLATE DATABASE_DEFAULT,
    ClassDescription = permission.class_desc COLLATE DATABASE_DEFAULT,
    SecurableSchema = CASE
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' THEN OBJECT_SCHEMA_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SCHEMA' THEN SCHEMA_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        ELSE NULL
    END,
    SecurableName = CASE
        WHEN permission.class_desc = N'DATABASE' THEN DB_NAME() COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' THEN OBJECT_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'SCHEMA' THEN SCHEMA_NAME(permission.major_id) COLLATE DATABASE_DEFAULT
        WHEN permission.class_desc = N'DATABASE_PRINCIPAL' THEN target_database_principal.name COLLATE DATABASE_DEFAULT
        ELSE CONVERT(nvarchar(256), permission.major_id)
    END,
    SecurableColumn = CASE
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' AND permission.minor_id > 0 THEN COL_NAME(permission.major_id, permission.minor_id) COLLATE DATABASE_DEFAULT
        ELSE NULL
    END,
    GranteeName = grantee.name COLLATE DATABASE_DEFAULT,
    GrantorName = grantor.name COLLATE DATABASE_DEFAULT
FROM sys.database_permissions AS permission
INNER JOIN sys.database_principals AS grantee ON grantee.principal_id = permission.grantee_principal_id
INNER JOIN sys.database_principals AS grantor ON grantor.principal_id = permission.grantor_principal_id
LEFT JOIN sys.database_principals AS target_database_principal ON target_database_principal.principal_id = permission.major_id AND permission.class_desc = N'DATABASE_PRINCIPAL'
WHERE (@principalName IS NULL OR grantee.name COLLATE DATABASE_DEFAULT = @principalName)
ORDER BY Scope, DatabaseName, GranteeName, PermissionName, ClassDescription, SecurableName, SecurableColumn;";

    private const string SqlServerInstancePropertiesManagementQuery = @"
SELECT Name, Value
FROM
(
    VALUES
        (N'MachineName', CONVERT(nvarchar(4000), SERVERPROPERTY(N'MachineName'))),
        (N'ServerName', CONVERT(nvarchar(4000), SERVERPROPERTY(N'ServerName'))),
        (N'InstanceName', CONVERT(nvarchar(4000), SERVERPROPERTY(N'InstanceName'))),
        (N'ProductVersion', CONVERT(nvarchar(4000), SERVERPROPERTY(N'ProductVersion'))),
        (N'ProductLevel', CONVERT(nvarchar(4000), SERVERPROPERTY(N'ProductLevel'))),
        (N'Edition', CONVERT(nvarchar(4000), SERVERPROPERTY(N'Edition'))),
        (N'EngineEdition', CONVERT(nvarchar(4000), SERVERPROPERTY(N'EngineEdition'))),
        (N'Collation', CONVERT(nvarchar(4000), SERVERPROPERTY(N'Collation'))),
        (N'IsClustered', CONVERT(nvarchar(4000), SERVERPROPERTY(N'IsClustered'))),
        (N'IsHadrEnabled', CONVERT(nvarchar(4000), SERVERPROPERTY(N'IsHadrEnabled')))
) AS properties(Name, Value)
WHERE (@name IS NULL OR Name = @name)
ORDER BY Name;";

    private const string SqlServerConfigurationsManagementQuery = @"
SELECT
    Name = name,
    Value = CONVERT(int, value),
    ValueInUse = CONVERT(int, value_in_use),
    Minimum = CONVERT(int, minimum),
    Maximum = CONVERT(int, maximum),
    IsDynamic = CONVERT(bit, is_dynamic),
    IsAdvanced = CONVERT(bit, is_advanced),
    Description = description
FROM sys.configurations
WHERE (@includeAdvanced = 1 OR is_advanced = 0)
  AND (@name IS NULL OR name = @name)
ORDER BY name;";

    private const string SqlServerDependenciesManagementQuery = @"
SELECT
    DependencyType = N'SqlExpression',
    ReferencingSchema = referencing_schema.name,
    ReferencingName = referencing_object.name,
    ReferencingType = referencing_object.type_desc,
    ReferencedServerName = dependency.referenced_server_name,
    ReferencedDatabaseName = dependency.referenced_database_name,
    ReferencedSchemaName = dependency.referenced_schema_name,
    ReferencedEntityName = dependency.referenced_entity_name,
    ReferencedClassDescription = dependency.referenced_class_desc,
    IsCallerDependent = CONVERT(bit, dependency.is_caller_dependent),
    IsAmbiguous = CONVERT(bit, dependency.is_ambiguous)
FROM sys.sql_expression_dependencies AS dependency
INNER JOIN sys.objects AS referencing_object ON referencing_object.object_id = dependency.referencing_id
INNER JOIN sys.schemas AS referencing_schema ON referencing_schema.schema_id = referencing_object.schema_id
WHERE (@schema IS NULL OR referencing_schema.name = @schema)
  AND (@name IS NULL OR referencing_object.name = @name)
UNION ALL
SELECT
    DependencyType = N'ForeignKey',
    ReferencingSchema = parent_schema.name,
    ReferencingName = parent_table.name,
    ReferencingType = parent_table.type_desc,
    ReferencedServerName = CONVERT(nvarchar(128), NULL),
    ReferencedDatabaseName = CONVERT(nvarchar(128), NULL),
    ReferencedSchemaName = referenced_schema.name,
    ReferencedEntityName = referenced_table.name,
    ReferencedClassDescription = N'OBJECT_OR_COLUMN',
    IsCallerDependent = CONVERT(bit, 0),
    IsAmbiguous = CONVERT(bit, 0)
FROM sys.foreign_keys AS foreign_key
INNER JOIN sys.tables AS parent_table ON parent_table.object_id = foreign_key.parent_object_id
INNER JOIN sys.schemas AS parent_schema ON parent_schema.schema_id = parent_table.schema_id
INNER JOIN sys.tables AS referenced_table ON referenced_table.object_id = foreign_key.referenced_object_id
INNER JOIN sys.schemas AS referenced_schema ON referenced_schema.schema_id = referenced_table.schema_id
WHERE (@schema IS NULL OR parent_schema.name = @schema)
  AND (@name IS NULL OR parent_table.name = @name)
ORDER BY ReferencingSchema, ReferencingName, DependencyType, ReferencedSchemaName, ReferencedEntityName;";

    private const string SqlServerModuleScriptsManagementQuery = @"
SELECT
    ScriptType = N'Module',
    SchemaName = schema_info.name,
    ObjectName = object_info.name,
    ObjectType = object_info.type_desc,
    Script = module_info.definition
FROM sys.objects AS object_info
INNER JOIN sys.schemas AS schema_info ON schema_info.schema_id = object_info.schema_id
INNER JOIN sys.sql_modules AS module_info ON module_info.object_id = object_info.object_id
WHERE object_info.type IN ('P', 'PC', 'X', 'V', 'TR', 'FN', 'IF', 'TF', 'FS', 'FT')
  AND (@schema IS NULL OR schema_info.name = @schema)
  AND (@name IS NULL OR object_info.name = @name)
ORDER BY schema_info.name, object_info.name;";

    private const string SqlServerTableScriptColumnsManagementQuery = @"
SELECT
    SchemaName = schema_info.name,
    TableName = table_info.name,
    ColumnName = column_info.name,
    Ordinal = column_info.column_id,
    DataType = CASE
        WHEN type_info.is_user_defined = 1 THEN QUOTENAME(type_schema.name) + N'.' + QUOTENAME(type_info.name)
        WHEN type_info.name = N'xml' AND column_info.xml_collection_id <> 0 THEN N'xml(' + CASE WHEN column_info.is_xml_document = 1 THEN N'DOCUMENT ' ELSE N'CONTENT ' END + QUOTENAME(xml_schema.name) + N'.' + QUOTENAME(xml_collection.name) + N')'
        WHEN type_info.name = N'xml' THEN N'xml'
        WHEN type_info.name IN (N'varchar', N'char', N'varbinary', N'binary') THEN type_info.name + N'(' + CASE WHEN column_info.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), column_info.max_length) END + N')'
        WHEN type_info.name IN (N'nvarchar', N'nchar') THEN type_info.name + N'(' + CASE WHEN column_info.max_length = -1 THEN N'max' ELSE CONVERT(nvarchar(12), column_info.max_length / 2) END + N')'
        WHEN type_info.name IN (N'decimal', N'numeric') THEN type_info.name + N'(' + CONVERT(nvarchar(12), column_info.precision) + N',' + CONVERT(nvarchar(12), column_info.scale) + N')'
        WHEN type_info.name = N'float' THEN type_info.name + N'(' + CONVERT(nvarchar(12), column_info.precision) + N')'
        WHEN type_info.name IN (N'datetime2', N'datetimeoffset', N'time') THEN type_info.name + N'(' + CONVERT(nvarchar(12), column_info.scale) + N')'
        ELSE type_info.name
    END + CASE
        WHEN type_info.name IN (N'varchar', N'char', N'nvarchar', N'nchar') AND column_info.collation_name IS NOT NULL THEN N' COLLATE ' + column_info.collation_name
        ELSE N''
    END + CASE
        WHEN column_info.is_filestream = 1 THEN N' FILESTREAM'
        ELSE N''
    END,
    IsNullable = CONVERT(bit, column_info.is_nullable),
    IsIdentity = CONVERT(bit, column_info.is_identity),
    IdentitySeed = CONVERT(nvarchar(40), identity_info.seed_value),
    IdentityIncrement = CONVERT(nvarchar(40), identity_info.increment_value),
    DefaultDefinition = default_info.definition,
    ComputedDefinition = computed_info.definition,
    IsPersisted = CONVERT(bit, ISNULL(computed_info.is_persisted, 0)),
    GeneratedAlwaysTypeDescription = column_info.generated_always_type_desc,
    TemporalType = table_info.temporal_type,
    HistoryTableSchema = history_schema.name,
    HistoryTableName = history_table.name,
    PrimaryKeyName = primary_key.name,
    PrimaryKeyOrdinal = primary_key_column.key_ordinal,
    PrimaryKeyIndexType = primary_key.type_desc,
    PrimaryKeyIsDescending = CONVERT(bit, primary_key_column.is_descending_key)
FROM sys.tables AS table_info
INNER JOIN sys.schemas AS schema_info ON schema_info.schema_id = table_info.schema_id
INNER JOIN sys.columns AS column_info ON column_info.object_id = table_info.object_id
INNER JOIN sys.types AS type_info ON type_info.user_type_id = column_info.user_type_id
INNER JOIN sys.schemas AS type_schema ON type_schema.schema_id = type_info.schema_id
LEFT JOIN sys.xml_schema_collections AS xml_collection ON xml_collection.xml_collection_id = column_info.xml_collection_id AND column_info.xml_collection_id <> 0
LEFT JOIN sys.schemas AS xml_schema ON xml_schema.schema_id = xml_collection.schema_id
LEFT JOIN sys.identity_columns AS identity_info ON identity_info.object_id = column_info.object_id AND identity_info.column_id = column_info.column_id
LEFT JOIN sys.default_constraints AS default_info ON default_info.object_id = column_info.default_object_id
LEFT JOIN sys.computed_columns AS computed_info ON computed_info.object_id = column_info.object_id AND computed_info.column_id = column_info.column_id
LEFT JOIN sys.tables AS history_table ON history_table.object_id = table_info.history_table_id
LEFT JOIN sys.schemas AS history_schema ON history_schema.schema_id = history_table.schema_id
LEFT JOIN sys.indexes AS primary_key ON primary_key.object_id = table_info.object_id AND primary_key.is_primary_key = 1
LEFT JOIN sys.index_columns AS primary_key_column ON primary_key_column.object_id = primary_key.object_id AND primary_key_column.index_id = primary_key.index_id AND primary_key_column.column_id = column_info.column_id
WHERE (@schema IS NULL OR schema_info.name = @schema)
  AND (@name IS NULL OR table_info.name = @name)
ORDER BY schema_info.name, table_info.name, column_info.column_id;";
}
