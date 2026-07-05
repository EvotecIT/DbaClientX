using DBAClientX.SqlServerMonitoring;

namespace DBAClientX.PowerShell;

/// <summary>Collects a SQL Server monitoring snapshot through the DbaClientX SQL Server provider.</summary>
/// <example>
/// <summary>Collect baseline monitoring data.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXSqlServerMonitoring -Server . -Database master -TrustServerCertificate</code>
/// <para>Returns a typed monitoring snapshot for the local SQL Server instance.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXSqlServerMonitoring")]
[CmdletBinding()]
public sealed class CmdletGetDbaXSqlServerMonitoring : AsyncPSCmdlet
{
    /// <summary>SQL Server instance name or address.</summary>
    [Parameter(Mandatory = true)]
    [Alias("SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Database used for connection-level checks.</summary>
    [Parameter(Mandatory = false)]
    public string Database { get; set; } = "master";

    /// <summary>Monitoring areas to collect.</summary>
    [Parameter(Mandatory = false)]
    public SqlServerMonitoringScope Scope { get; set; } = SqlServerMonitoringScope.Baseline;

    /// <summary>Optional TCP port.</summary>
    [Parameter(Mandatory = false)]
    public int? Port { get; set; }

    /// <summary>Optional SQL login name.</summary>
    [Parameter(Mandatory = false)]
    public string? Username { get; set; }

    /// <summary>Optional SQL login password.</summary>
    [Parameter(Mandatory = false)]
    public string? Password { get; set; }

    /// <summary>Optional SQL credential.</summary>
    [Parameter(Mandatory = false)]
    [Credential]
    public PSCredential? Credential { get; set; }

    /// <summary>Trusts the SQL Server certificate while keeping encryption enabled.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter TrustServerCertificate { get; set; }

    /// <summary>Includes system databases in database-level collectors.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeSystemDatabases { get; set; }

    /// <summary>Includes disabled SQL Server Agent jobs.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeDisabledAgentJobs { get; set; }

    /// <summary>Maximum age in hours for full backups before status is considered overdue.</summary>
    [Parameter(Mandatory = false)]
    public double? MaxFullBackupAgeHours { get; set; }

    /// <summary>Maximum age in hours for differential backups before status is considered overdue.</summary>
    [Parameter(Mandatory = false)]
    public double? MaxDifferentialBackupAgeHours { get; set; }

    /// <summary>Maximum age in minutes for log backups before status is considered overdue.</summary>
    [Parameter(Mandatory = false)]
    public double? MaxLogBackupAgeMinutes { get; set; }

    /// <summary>Maximum age in days for last known good CHECKDB before status is considered overdue.</summary>
    [Parameter(Mandatory = false)]
    public double? MaxCheckDbAgeDays { get; set; }

    /// <summary>Maximum cumulative wait percentage to include in wait statistics.</summary>
    [Parameter(Mandatory = false)]
    public decimal WaitStatisticThresholdPercent { get; set; } = 95m;

    /// <summary>Optional connection timeout in seconds.</summary>
    [Parameter(Mandatory = false)]
    public int? ConnectTimeoutSeconds { get; set; }

    /// <summary>Optional SQL Server application name.</summary>
    [Parameter(Mandatory = false)]
    public string? ApplicationName { get; set; } = "DbaClientX.PowerShell";

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var (username, password, integratedSecurity) = PowerShellHelpers.ResolveSqlServerCredential(Username, Password, Credential);
        var target = new SqlServerMonitoringTarget
        {
            ServerOrInstance = Server,
            Database = Database,
            IntegratedSecurity = integratedSecurity,
            Username = username,
            Password = password,
            Port = Port,
            TrustServerCertificate = TrustServerCertificate.IsPresent,
            ConnectTimeoutSeconds = ConnectTimeoutSeconds,
            ApplicationName = ApplicationName
        };

        var options = new SqlServerMonitoringOptions
        {
            Scope = Scope,
            IncludeSystemDatabases = IncludeSystemDatabases.IsPresent,
            IncludeDisabledAgentJobs = IncludeDisabledAgentJobs.IsPresent,
            WaitStatisticThresholdPercent = WaitStatisticThresholdPercent
        };

        if (MaxFullBackupAgeHours.HasValue)
        {
            options.MaxFullBackupAge = TimeSpan.FromHours(MaxFullBackupAgeHours.Value);
        }

        if (MaxDifferentialBackupAgeHours.HasValue)
        {
            options.MaxDifferentialBackupAge = TimeSpan.FromHours(MaxDifferentialBackupAgeHours.Value);
        }

        if (MaxLogBackupAgeMinutes.HasValue)
        {
            options.MaxLogBackupAge = TimeSpan.FromMinutes(MaxLogBackupAgeMinutes.Value);
        }

        if (MaxCheckDbAgeDays.HasValue)
        {
            options.MaxCheckDbAge = TimeSpan.FromDays(MaxCheckDbAgeDays.Value);
        }

        using var client = new DBAClientX.SqlServer();
        WriteObject(await client.GetMonitoringSnapshotAsync(target, options, CancelToken).ConfigureAwait(false));
    }
}
