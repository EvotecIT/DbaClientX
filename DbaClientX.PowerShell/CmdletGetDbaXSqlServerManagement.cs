using DBAClientX.SqlServerManagement;

namespace DBAClientX.PowerShell;

/// <summary>Gets SQL Server-specific management metadata without requiring SQL Server Management Objects.</summary>
/// <para>Returns SQL Server Agent, security, dependency, scripting, copy-plan, inventory, instance property, and configuration metadata using native SQL Server catalog queries.</para>
/// <example>
/// <summary>List SQL Server Agent jobs.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXSqlServerManagement -Type AgentJob -ConnectionString 'Server=.;Database=msdb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True'</code>
/// <para>Lists SQL Server Agent jobs visible through the supplied connection.</para>
/// </example>
/// <example>
/// <summary>List database principals.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXSqlServerManagement -Type DatabasePrincipal -ConnectionString 'Server=.;Database=AppDb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True'</code>
/// <para>Lists database principals in the current database.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXSqlServerManagement")]
[CmdletBinding()]
public sealed class CmdletGetDbaXSqlServerManagement : AsyncPSCmdlet
{
    /// <summary>Selects the SQL Server management metadata type to return.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public DbaXSqlServerManagementType Type { get; set; }

    /// <summary>Specifies a SQL Server connection string.</summary>
    [Parameter(Mandatory = true, Position = 1)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Optional name filter used by Agent jobs, principals, instance properties, and configurations.</summary>
    [Parameter(Mandatory = false)]
    public string? Name { get; set; }

    /// <summary>Optional schema filter used by dependency, scripting, and copy-plan metadata.</summary>
    [Parameter(Mandatory = false)]
    public string? Schema { get; set; }

    /// <summary>Source table name for table copy plans.</summary>
    [Parameter(Mandatory = false)]
    public string? Table { get; set; }

    /// <summary>Destination schema name for table copy plans.</summary>
    [Parameter(Mandatory = false)]
    public string? DestinationSchema { get; set; }

    /// <summary>Destination table name for table copy plans.</summary>
    [Parameter(Mandatory = false)]
    public string? DestinationTable { get; set; }

    /// <summary>Optional role name filter for role membership metadata.</summary>
    [Parameter(Mandatory = false)]
    public string? RoleName { get; set; }

    /// <summary>Optional member name filter for role membership metadata.</summary>
    [Parameter(Mandatory = false)]
    public string? MemberName { get; set; }

    /// <summary>Optional grantee principal name filter for permission metadata.</summary>
    [Parameter(Mandatory = false)]
    public string? PrincipalName { get; set; }

    /// <summary>Includes disabled SQL Server Agent jobs or schedules where applicable.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeDisabled { get; set; }

    /// <summary>Includes system principals where applicable.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeSystem { get; set; }

    /// <summary>Includes advanced SQL Server configuration values.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeAdvanced { get; set; }

    /// <summary>Processes the SQL Server management metadata request.</summary>
    protected override async Task ProcessRecordAsync()
    {
        await Task.Yield();
        using var client = new DBAClientX.SqlServer();
        object result = Type switch
        {
            DbaXSqlServerManagementType.AgentJob => client.GetSqlServerAgentJobs(ConnectionString, Name, IncludeDisabled.IsPresent),
            DbaXSqlServerManagementType.AgentJobStep => client.GetSqlServerAgentJobSteps(ConnectionString, Name),
            DbaXSqlServerManagementType.AgentSchedule => client.GetSqlServerAgentSchedules(ConnectionString, Name, IncludeDisabled.IsPresent),
            DbaXSqlServerManagementType.ServerPrincipal => client.GetSqlServerServerPrincipals(ConnectionString, Name, IncludeSystem.IsPresent),
            DbaXSqlServerManagementType.DatabasePrincipal => client.GetSqlServerDatabasePrincipals(ConnectionString, Name, IncludeSystem.IsPresent),
            DbaXSqlServerManagementType.RoleMembership => client.GetSqlServerRoleMemberships(ConnectionString, RoleName, MemberName),
            DbaXSqlServerManagementType.Permission => client.GetSqlServerPermissions(ConnectionString, PrincipalName),
            DbaXSqlServerManagementType.InstanceProperty => client.GetSqlServerInstanceProperties(ConnectionString, Name),
            DbaXSqlServerManagementType.Configuration => client.GetSqlServerConfigurations(ConnectionString, Name, IncludeAdvanced.IsPresent),
            DbaXSqlServerManagementType.Dependency => client.GetSqlServerDependencies(ConnectionString, Schema, Name),
            DbaXSqlServerManagementType.ModuleScript => client.GetSqlServerModuleScripts(ConnectionString, Schema, Name),
            DbaXSqlServerManagementType.TableScript => client.GetSqlServerTableScripts(ConnectionString, Schema, Name),
            DbaXSqlServerManagementType.TableCopyPlan => GetTableCopyPlan(client),
            DbaXSqlServerManagementType.Inventory => client.GetSqlServerInventory(ConnectionString, IncludeSystem.IsPresent, IncludeAdvanced.IsPresent, IncludeDisabled.IsPresent),
            _ => throw new NotSupportedException($"SQL Server management type '{Type}' is not supported.")
        };

        WriteObject(result, enumerateCollection: true);
    }

    private SqlServerTableCopyPlan GetTableCopyPlan(DBAClientX.SqlServer client)
    {
        string schema = Schema ?? "dbo";
        string table = Table ?? Name ?? throw new PSArgumentException("Table or Name is required for TableCopyPlan.", nameof(Table));
        return client.GetSqlServerTableCopyPlan(ConnectionString, schema, table, DestinationSchema, DestinationTable);
    }
}
