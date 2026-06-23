using System.Collections.Generic;
using DBAClientX.SqlServerManagement;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>
    /// Lists SQL Server Agent job definitions from msdb.
    /// </summary>
    public virtual IReadOnlyList<SqlServerAgentJobInfo> GetSqlServerAgentJobs(
        string connectionString,
        string? jobName = null,
        bool includeDisabled = true)
        => ExecuteMetadata(connectionString, SqlServerAgentJobsManagementQuery, SqlServerManagementMappers.MapAgentJob, new Dictionary<string, object?>
        {
            ["@jobName"] = jobName,
            ["@includeDisabled"] = includeDisabled ? 1 : 0
        });

    /// <summary>
    /// Lists SQL Server Agent job step definitions from msdb.
    /// </summary>
    public virtual IReadOnlyList<SqlServerAgentJobStepInfo> GetSqlServerAgentJobSteps(
        string connectionString,
        string? jobName = null)
        => ExecuteMetadata(connectionString, SqlServerAgentJobStepsManagementQuery, SqlServerManagementMappers.MapAgentJobStep, new Dictionary<string, object?>
        {
            ["@jobName"] = jobName
        });

    /// <summary>
    /// Lists SQL Server Agent schedules from msdb.
    /// </summary>
    public virtual IReadOnlyList<SqlServerAgentScheduleInfo> GetSqlServerAgentSchedules(
        string connectionString,
        string? jobName = null,
        bool includeDisabled = true)
        => ExecuteMetadata(connectionString, SqlServerAgentSchedulesManagementQuery, SqlServerManagementMappers.MapAgentSchedule, new Dictionary<string, object?>
        {
            ["@jobName"] = jobName,
            ["@includeDisabled"] = includeDisabled ? 1 : 0
        });

    /// <summary>
    /// Lists SQL Server server-level principals.
    /// </summary>
    public virtual IReadOnlyList<SqlServerPrincipalInfo> GetSqlServerServerPrincipals(
        string connectionString,
        string? name = null,
        bool includeSystem = false)
        => ExecuteMetadata(connectionString, SqlServerServerPrincipalsManagementQuery, SqlServerManagementMappers.MapPrincipal, new Dictionary<string, object?>
        {
            ["@name"] = name,
            ["@includeSystem"] = includeSystem ? 1 : 0
        });

    /// <summary>
    /// Lists SQL Server database-level principals for the current database.
    /// </summary>
    public virtual IReadOnlyList<SqlServerPrincipalInfo> GetSqlServerDatabasePrincipals(
        string connectionString,
        string? name = null,
        bool includeSystem = false)
        => ExecuteMetadata(connectionString, SqlServerDatabasePrincipalsManagementQuery, SqlServerManagementMappers.MapPrincipal, new Dictionary<string, object?>
        {
            ["@name"] = name,
            ["@includeSystem"] = includeSystem ? 1 : 0
        });

    /// <summary>
    /// Lists SQL Server server-level and current database role memberships.
    /// </summary>
    public virtual IReadOnlyList<SqlServerRoleMembershipInfo> GetSqlServerRoleMemberships(
        string connectionString,
        string? roleName = null,
        string? memberName = null)
        => ExecuteMetadata(connectionString, SqlServerRoleMembershipsManagementQuery, SqlServerManagementMappers.MapRoleMembership, new Dictionary<string, object?>
        {
            ["@roleName"] = roleName,
            ["@memberName"] = memberName
        });

    /// <summary>
    /// Lists SQL Server server-level and current database permissions.
    /// </summary>
    public virtual IReadOnlyList<SqlServerPermissionInfo> GetSqlServerPermissions(
        string connectionString,
        string? principalName = null)
        => ExecuteMetadata(connectionString, SqlServerPermissionsManagementQuery, SqlServerManagementMappers.MapPermission, new Dictionary<string, object?>
        {
            ["@principalName"] = principalName
        });

    /// <summary>
    /// Lists selected SQL Server instance properties from SERVERPROPERTY.
    /// </summary>
    public virtual IReadOnlyList<SqlServerInstancePropertyInfo> GetSqlServerInstanceProperties(
        string connectionString,
        string? name = null)
        => ExecuteMetadata(connectionString, SqlServerInstancePropertiesManagementQuery, SqlServerManagementMappers.MapInstanceProperty, new Dictionary<string, object?>
        {
            ["@name"] = name
        });

    /// <summary>
    /// Lists SQL Server instance configuration values from sys.configurations.
    /// </summary>
    public virtual IReadOnlyList<SqlServerConfigurationInfo> GetSqlServerConfigurations(
        string connectionString,
        string? name = null,
        bool includeAdvanced = false)
        => ExecuteMetadata(connectionString, SqlServerConfigurationsManagementQuery, SqlServerManagementMappers.MapConfiguration, new Dictionary<string, object?>
        {
            ["@name"] = name,
            ["@includeAdvanced"] = includeAdvanced ? 1 : 0
        });
}
