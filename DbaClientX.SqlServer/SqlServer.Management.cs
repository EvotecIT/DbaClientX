using System;
using System.Collections.Generic;
using DBAClientX.SqlServerManagement;
using Microsoft.Data.SqlClient;

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
    {
        ValidateConnectionString(connectionString);
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
            bool includeAvailabilityGroups = SupportsAvailabilityGroups(connection, transaction);
            bool includeServerPermissions = SupportsServerPermissions(connection, transaction);
            bool includeFullTextStoplists = SupportsFullTextStoplists(connection, transaction);
            bool includeSearchPropertyLists = SupportsSearchPropertyLists(connection, transaction);
            bool includeDatabaseScopedCredentials = SupportsDatabaseScopedCredentials(connection, transaction);
            bool includeExternalLanguages = SupportsExternalLanguages(connection, transaction);
            string query = BuildSqlServerPermissionsManagementQuery(
                includeAvailabilityGroups,
                includeServerPermissions,
                includeFullTextStoplists,
                includeSearchPropertyLists,
                includeDatabaseScopedCredentials,
                includeExternalLanguages);
            return ExecuteMappedQuery(connection, transaction, query, SqlServerManagementMappers.MapPermission, parameters: new Dictionary<string, object?>
            {
                ["@principalName"] = principalName
            });
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

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

    /// <summary>
    /// Lists SQL Server object dependencies from native catalog views.
    /// </summary>
    public virtual IReadOnlyList<SqlServerDependencyInfo> GetSqlServerDependencies(
        string connectionString,
        string? schema = null,
        string? name = null)
    {
        ValidateConnectionString(connectionString);
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
            string query = BuildSqlServerDependenciesManagementQuery(SupportsServerTriggers(connection, transaction));
            return ExecuteMappedQuery(connection, transaction, query, SqlServerManagementMappers.MapDependency, parameters: new Dictionary<string, object?>
            {
                ["@schema"] = schema,
                ["@name"] = name
            });
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private static string BuildSqlServerDependenciesManagementQuery(bool includeServerTriggers)
        => SqlServerDependenciesManagementQuery.Replace(
            SqlServerDependenciesServerTriggerUnionToken,
            includeServerTriggers ? SqlServerDependenciesServerTriggerUnion : string.Empty);

    /// <summary>
    /// Lists SQL Server module definitions for procedures, functions, views, and triggers.
    /// </summary>
    public virtual IReadOnlyList<SqlServerScriptInfo> GetSqlServerModuleScripts(
        string connectionString,
        string? schema = null,
        string? name = null)
    {
        ValidateConnectionString(connectionString);
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
            string query = BuildSqlServerModuleScriptsManagementQuery(
                SupportsServerTriggerModules(connection, transaction),
                SupportsDatabaseClrModules(connection, transaction),
                SupportsDatabaseClrFunctionOrdering(connection, transaction));
            return ExecuteMappedQuery(connection, transaction, query, SqlServerManagementMappers.MapScript, parameters: new Dictionary<string, object?>
            {
                ["@schema"] = schema,
                ["@name"] = name
            });
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private static string BuildSqlServerModuleScriptsManagementQuery(bool includeServerTriggerModules, bool includeDatabaseClrModules, bool includeDatabaseClrFunctionOrdering)
        => SqlServerModuleScriptsManagementQuery
            .Replace(
                SqlServerModuleScriptsDatabaseClrUnionToken,
                includeDatabaseClrModules ? SqlServerModuleScriptsDatabaseClrUnion : string.Empty)
            .Replace(
                SqlServerModuleScriptsDatabaseClrFunctionOrderClauseToken,
                includeDatabaseClrModules && includeDatabaseClrFunctionOrdering ? SqlServerModuleScriptsDatabaseClrFunctionOrderClause : string.Empty)
            .Replace(
                SqlServerModuleScriptsDatabaseClrFunctionOrderApplyToken,
                includeDatabaseClrModules && includeDatabaseClrFunctionOrdering ? SqlServerModuleScriptsDatabaseClrFunctionOrderApply : string.Empty)
            .Replace(
                SqlServerModuleScriptsServerTriggerUnionToken,
                includeServerTriggerModules ? SqlServerModuleScriptsServerTriggerUnion : string.Empty);

    private bool SupportsServerTriggerModules(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerServerTriggerModulesSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsDatabaseClrModules(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerDatabaseClrModulesSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsDatabaseClrFunctionOrdering(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerDatabaseClrFunctionOrderingSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsServerTriggers(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerServerTriggersSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    /// <summary>
    /// Generates basic SQL Server CREATE TABLE scripts from catalog metadata.
    /// </summary>
    public virtual IReadOnlyList<SqlServerScriptInfo> GetSqlServerTableScripts(
        string connectionString,
        string? schema = null,
        string? name = null)
    {
        IReadOnlyList<SqlServerTableColumnScriptInfo> columns = GetSqlServerTableScriptColumns(connectionString, schema, name, includeGraphHiddenColumns: false, includeGraphTableOnlyRows: true);
        return SqlServerManagementScripting.BuildTableScripts(columns);
    }

    /// <summary>
    /// Builds an inspectable SQL Server table copy and sync command plan from metadata.
    /// </summary>
    public virtual SqlServerTableCopyPlan GetSqlServerTableCopyPlan(
        string sourceConnectionString,
        string sourceSchema,
        string sourceTable,
        string? destinationSchema = null,
        string? destinationTable = null)
    {
        IReadOnlyList<SqlServerTableColumnScriptInfo> columns = GetSqlServerTableScriptColumns(sourceConnectionString, sourceSchema, sourceTable, includeGraphHiddenColumns: true, includeGraphTableOnlyRows: false);
        IReadOnlyList<Metadata.DbaIndexInfo> indexes = GetIndexes(sourceConnectionString, sourceSchema, sourceTable);
        return SqlServerManagementScripting.BuildTableCopyPlan(
            sourceSchema,
            sourceTable,
            destinationSchema ?? sourceSchema,
            destinationTable ?? sourceTable,
            columns,
            indexes);
    }

    /// <summary>
    /// Builds a SQL Server instance inventory snapshot from reusable management readers.
    /// </summary>
    public virtual SqlServerInventorySnapshot GetSqlServerInventory(
        string connectionString,
        bool includeSystem = false,
        bool includeAdvanced = false,
        bool includeDisabledAgent = true)
    {
        var snapshot = new SqlServerInventorySnapshot
        {
            InstanceProperties = GetSqlServerInstanceProperties(connectionString),
            Configurations = GetOptionalSqlServerConfigurations(connectionString, includeAdvanced),
            Databases = GetDatabases(connectionString),
            ServerPrincipals = GetSqlServerServerPrincipals(connectionString, includeSystem: includeSystem)
        };

        if (SupportsAgentCatalog(connectionString))
        {
            snapshot.AgentJobs = GetSqlServerAgentJobs(connectionString, includeDisabled: includeDisabledAgent);
        }

        return snapshot;
    }

    private IReadOnlyList<SqlServerConfigurationInfo> GetOptionalSqlServerConfigurations(string connectionString, bool includeAdvanced)
    {
        try
        {
            return GetSqlServerConfigurations(connectionString, includeAdvanced: includeAdvanced);
        }
        catch (SqlException)
        {
            return Array.Empty<SqlServerConfigurationInfo>();
        }
    }

    private bool SupportsAgentCatalog(string connectionString)
    {
        ValidateConnectionString(connectionString);
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
            object? result = ExecuteScalar(connection, transaction, SqlServerAgentCatalogSupportQuery);
            return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private IReadOnlyList<SqlServerTableColumnScriptInfo> GetSqlServerTableScriptColumns(
        string connectionString,
        string? schema,
        string? name,
        bool includeGraphHiddenColumns = false,
        bool includeGraphTableOnlyRows = true)
    {
        ValidateConnectionString(connectionString);
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
            bool includeMasking = SupportsMaskedColumns(connection, transaction);
            bool includeEncryption = SupportsColumnEncryption(connection, transaction);
            bool includeGraphEdgeConstraints = SupportsGraphEdgeConstraints(connection, transaction);
            bool includeHashIndexes = SupportsHashIndexes(connection, transaction);
            bool includeFileTables = SupportsFileTables(connection, transaction);
            string query = BuildSqlServerTableScriptColumnsManagementQuery(
                includeMasking,
                includeEncryption,
                includeGraphEdgeConstraints,
                includeHashIndexes,
                includeFileTables,
                includeGraphHiddenColumns,
                includeGraphTableOnlyRows);
            return ExecuteMappedQuery(connection, transaction, query, SqlServerManagementMappers.MapTableScriptColumn, parameters: new Dictionary<string, object?>
            {
                ["@schema"] = schema,
                ["@name"] = name
            });
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private bool SupportsMaskedColumns(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerMaskedColumnsSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsColumnEncryption(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerColumnEncryptionSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsAvailabilityGroups(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerAvailabilityGroupsSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsGraphEdgeConstraints(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerGraphEdgeConstraintsSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsHashIndexes(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerHashIndexesSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsFileTables(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerFileTablesSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsServerPermissions(SqlConnection connection, SqlTransaction? transaction)
    {
        object? result = ExecuteScalar(connection, transaction, SqlServerServerPermissionsSupportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private bool SupportsFullTextStoplists(SqlConnection connection, SqlTransaction? transaction)
        => SupportsCatalogView(connection, transaction, SqlServerFullTextStoplistsSupportQuery);

    private bool SupportsSearchPropertyLists(SqlConnection connection, SqlTransaction? transaction)
        => SupportsCatalogView(connection, transaction, SqlServerSearchPropertyListsSupportQuery);

    private bool SupportsDatabaseScopedCredentials(SqlConnection connection, SqlTransaction? transaction)
        => SupportsCatalogView(connection, transaction, SqlServerDatabaseScopedCredentialsSupportQuery);

    private bool SupportsExternalLanguages(SqlConnection connection, SqlTransaction? transaction)
        => SupportsCatalogView(connection, transaction, SqlServerExternalLanguagesSupportQuery);

    private bool SupportsCatalogView(SqlConnection connection, SqlTransaction? transaction, string supportQuery)
    {
        object? result = ExecuteScalar(connection, transaction, supportQuery);
        return result is not null && result is not DBNull && Convert.ToInt32(result) == 1;
    }

    private static string BuildSqlServerPermissionsManagementQuery(
        bool includeAvailabilityGroups,
        bool includeServerPermissions,
        bool includeFullTextStoplists,
        bool includeSearchPropertyLists,
        bool includeDatabaseScopedCredentials,
        bool includeExternalLanguages)
        => SqlServerPermissionsManagementQuery
            .Replace(
                SqlServerPermissionsServerUnionToken,
                includeServerPermissions ? SqlServerPermissionsServerUnion : string.Empty)
            .Replace(
                SqlServerPermissionsAvailabilityGroupNameToken,
                includeAvailabilityGroups ? SqlServerPermissionsAvailabilityGroupNameProjection : SqlServerPermissionsLegacyAvailabilityGroupNameProjection)
            .Replace(
                SqlServerPermissionsAvailabilityGroupJoinToken,
                includeAvailabilityGroups ? SqlServerPermissionsAvailabilityGroupJoin : string.Empty)
            .Replace(
                SqlServerPermissionsAdvancedDatabaseSecurableCasesToken,
                string.Concat(
                    includeFullTextStoplists ? SqlServerPermissionsFullTextStoplistCase : string.Empty,
                    includeSearchPropertyLists ? SqlServerPermissionsSearchPropertyListCase : string.Empty,
                    includeDatabaseScopedCredentials ? SqlServerPermissionsDatabaseScopedCredentialCase : string.Empty,
                    includeExternalLanguages ? SqlServerPermissionsExternalLanguageCase : string.Empty))
            .Replace(
                SqlServerPermissionsAdvancedDatabaseSecurableJoinsToken,
                string.Concat(
                    includeFullTextStoplists ? SqlServerPermissionsFullTextStoplistJoin : string.Empty,
                    includeSearchPropertyLists ? SqlServerPermissionsSearchPropertyListJoin : string.Empty,
                    includeDatabaseScopedCredentials ? SqlServerPermissionsDatabaseScopedCredentialJoin : string.Empty,
                    includeExternalLanguages ? SqlServerPermissionsExternalLanguageJoin : string.Empty));

    private static string BuildSqlServerTableScriptColumnsManagementQuery(
        bool includeMasking,
        bool includeEncryption,
        bool includeGraphEdgeConstraints,
        bool includeHashIndexes,
        bool includeFileTables,
        bool includeGraphHiddenColumns,
        bool includeGraphTableOnlyRows)
        => SqlServerTableScriptColumnsManagementQuery
            .Replace(
                SqlServerTableScriptMaskingFunctionToken,
                includeMasking ? SqlServerTableScriptMaskingFunctionProjection : SqlServerTableScriptLegacyMaskingFunctionProjection)
            .Replace(
                SqlServerTableScriptMaskingJoinToken,
                includeMasking ? SqlServerTableScriptMaskingJoin : string.Empty)
            .Replace(
                SqlServerTableScriptEncryptionDefinitionToken,
                includeEncryption ? SqlServerTableScriptEncryptionDefinitionProjection : SqlServerTableScriptLegacyEncryptionDefinitionProjection)
            .Replace(
                SqlServerTableScriptEncryptionJoinToken,
                includeEncryption ? SqlServerTableScriptEncryptionJoin : string.Empty)
            .Replace(
                SqlServerTableScriptPrimaryKeyBucketCountToken,
                includeHashIndexes ? SqlServerTableScriptPrimaryKeyBucketCountProjection : SqlServerTableScriptLegacyPrimaryKeyBucketCountProjection)
            .Replace(
                SqlServerTableScriptPrimaryKeyHashJoinToken,
                includeHashIndexes ? SqlServerTableScriptPrimaryKeyHashJoin : string.Empty)
            .Replace(
                SqlServerTableScriptUniqueHashJoinToken,
                includeHashIndexes ? SqlServerTableScriptUniqueHashJoin : string.Empty)
            .Replace(
                SqlServerTableScriptUniqueHashBucketCountToken,
                includeHashIndexes ? SqlServerTableScriptUniqueHashBucketCount : "N''")
            .Replace(
                SqlServerTableScriptMemoryHashJoinToken,
                includeHashIndexes ? SqlServerTableScriptMemoryHashJoin : string.Empty)
            .Replace(
                SqlServerTableScriptMemoryHashBucketCountToken,
                includeHashIndexes ? SqlServerTableScriptMemoryHashBucketCount : "N''")
            .Replace(
                SqlServerTableScriptFileTableOptionsToken,
                includeFileTables ? SqlServerTableScriptFileTableOptionsProjection : SqlServerTableScriptLegacyFileTableOptionsProjection)
            .Replace(
                SqlServerTableScriptFileTableJoinToken,
                includeFileTables ? SqlServerTableScriptFileTableJoin : string.Empty)
            .Replace(
                SqlServerTableScriptGraphHiddenColumnFilterToken,
                includeGraphHiddenColumns ? SqlServerTableScriptGraphCopyColumnFilter : SqlServerTableScriptGraphHiddenColumnFilter)
            .Replace(
                SqlServerTableScriptGraphTableOnlyRowsToken,
                includeGraphTableOnlyRows ? SqlServerTableScriptGraphTableOnlyRows : string.Empty)
            .Replace(
                SqlServerTableScriptGraphEdgeConstraintStatementsToken,
                includeGraphEdgeConstraints ? SqlServerTableScriptGraphEdgeConstraintStatements : string.Empty);
}
