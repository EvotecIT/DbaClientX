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
  AND (@name IS NULL OR sp.name = @name)
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
    Scope = N'Server',
    DatabaseName = CONVERT(nvarchar(128), NULL),
    RoleName = role_principal.name,
    RoleTypeDescription = role_principal.type_desc,
    MemberName = member_principal.name,
    MemberTypeDescription = member_principal.type_desc
FROM sys.server_role_members AS membership
INNER JOIN sys.server_principals AS role_principal ON role_principal.principal_id = membership.role_principal_id
INNER JOIN sys.server_principals AS member_principal ON member_principal.principal_id = membership.member_principal_id
WHERE (@roleName IS NULL OR role_principal.name = @roleName)
  AND (@memberName IS NULL OR member_principal.name = @memberName)
UNION ALL
SELECT
    Scope = N'Database',
    DatabaseName = DB_NAME(),
    RoleName = role_principal.name,
    RoleTypeDescription = role_principal.type_desc,
    MemberName = member_principal.name,
    MemberTypeDescription = member_principal.type_desc
FROM sys.database_role_members AS membership
INNER JOIN sys.database_principals AS role_principal ON role_principal.principal_id = membership.role_principal_id
INNER JOIN sys.database_principals AS member_principal ON member_principal.principal_id = membership.member_principal_id
WHERE (@roleName IS NULL OR role_principal.name = @roleName)
  AND (@memberName IS NULL OR member_principal.name = @memberName)
ORDER BY Scope, DatabaseName, RoleName, MemberName;";

    private const string SqlServerPermissionsManagementQuery = @"
SELECT
    Scope = N'Server',
    DatabaseName = CONVERT(nvarchar(128), NULL),
    State = permission.state,
    StateDescription = permission.state_desc,
    PermissionName = permission.permission_name,
    ClassDescription = permission.class_desc,
    SecurableSchema = CONVERT(nvarchar(128), NULL),
    SecurableName = CASE permission.class_desc
        WHEN N'SERVER' THEN CONVERT(nvarchar(128), SERVERPROPERTY(N'ServerName'))
        WHEN N'ENDPOINT' THEN endpoint.name
        ELSE CONVERT(nvarchar(256), permission.major_id)
    END,
    GranteeName = grantee.name,
    GrantorName = grantor.name
FROM sys.server_permissions AS permission
INNER JOIN sys.server_principals AS grantee ON grantee.principal_id = permission.grantee_principal_id
INNER JOIN sys.server_principals AS grantor ON grantor.principal_id = permission.grantor_principal_id
LEFT JOIN sys.endpoints AS endpoint ON endpoint.endpoint_id = permission.major_id AND permission.class_desc = N'ENDPOINT'
WHERE (@principalName IS NULL OR grantee.name = @principalName)
UNION ALL
SELECT
    Scope = N'Database',
    DatabaseName = DB_NAME(),
    State = permission.state,
    StateDescription = permission.state_desc,
    PermissionName = permission.permission_name,
    ClassDescription = permission.class_desc,
    SecurableSchema = CASE
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' THEN OBJECT_SCHEMA_NAME(permission.major_id)
        WHEN permission.class_desc = N'SCHEMA' THEN SCHEMA_NAME(permission.major_id)
        ELSE NULL
    END,
    SecurableName = CASE
        WHEN permission.class_desc = N'DATABASE' THEN DB_NAME()
        WHEN permission.class_desc = N'OBJECT_OR_COLUMN' THEN OBJECT_NAME(permission.major_id)
        WHEN permission.class_desc = N'SCHEMA' THEN SCHEMA_NAME(permission.major_id)
        ELSE CONVERT(nvarchar(256), permission.major_id)
    END,
    GranteeName = grantee.name,
    GrantorName = grantor.name
FROM sys.database_permissions AS permission
INNER JOIN sys.database_principals AS grantee ON grantee.principal_id = permission.grantee_principal_id
INNER JOIN sys.database_principals AS grantor ON grantor.principal_id = permission.grantor_principal_id
WHERE (@principalName IS NULL OR grantee.name = @principalName)
ORDER BY Scope, DatabaseName, GranteeName, PermissionName, ClassDescription, SecurableName;";

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
}
