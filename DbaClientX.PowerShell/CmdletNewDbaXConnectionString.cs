namespace DBAClientX.PowerShell;

/// <summary>Builds a provider connection string using the matching DbaClientX C# provider.</summary>
/// <example>
/// <summary>Create a SQL Server connection string.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXConnectionString -Provider SqlServer -Server . -Database master -TrustServerCertificate</code>
/// <para>Returns a SQL Server connection string using integrated security.</para>
/// </example>
[Cmdlet(VerbsCommon.New, "DbaXConnectionString")]
[CmdletBinding()]
public sealed class CmdletNewDbaXConnectionString : PSCmdlet
{
    /// <summary>Database provider whose connection-string builder should be used.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Server, host, or SQL Server instance name. SQLite ignores this parameter.</summary>
    [Parameter(Mandatory = false)]
    public string Server { get; set; } = string.Empty;

    /// <summary>Database name, Oracle service name, or SQLite database path.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>Optional provider TCP port.</summary>
    [Parameter(Mandatory = false)]
    public int? Port { get; set; }

    /// <summary>Optional username. SQL Server uses integrated security when no credential or username/password is supplied.</summary>
    [Parameter(Mandatory = false)]
    public string? Username { get; set; }

    /// <summary>Optional password.</summary>
    [Parameter(Mandatory = false)]
    public string? Password { get; set; }

    /// <summary>Optional credential used instead of Username and Password.</summary>
    [Parameter(Mandatory = false)]
    [Credential]
    public PSCredential? Credential { get; set; }

    /// <summary>Enables provider SSL or encryption when supported by the provider builder.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter Ssl { get; set; }

    /// <summary>Trusts the SQL Server certificate when SQL Server encryption is enabled.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter TrustServerCertificate { get; set; }

    /// <summary>Builds a read-only SQLite connection string.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter ReadOnly { get; set; }

    /// <summary>Optional SQLite busy timeout in milliseconds.</summary>
    [Parameter(Mandatory = false)]
    public int? BusyTimeoutMs { get; set; }

    /// <summary>Optional connection timeout in seconds where supported.</summary>
    [Parameter(Mandatory = false)]
    public int? ConnectTimeoutSeconds { get; set; }

    /// <summary>Optional SQL Server application name.</summary>
    [Parameter(Mandatory = false)]
    public string? ApplicationName { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        if (Provider != DbaXProvider.SQLite && string.IsNullOrWhiteSpace(Server))
        {
            throw new PSArgumentException("Server is required for non-SQLite providers.", nameof(Server));
        }

        if (Port.HasValue && Port.Value <= 0)
        {
            throw new PSArgumentException("Port must be greater than zero.", nameof(Port));
        }

        if (BusyTimeoutMs.HasValue && BusyTimeoutMs.Value < 0)
        {
            throw new PSArgumentException("BusyTimeoutMs cannot be negative.", nameof(BusyTimeoutMs));
        }

        string? username = Username;
        string? password = Password;
        if (Credential != null)
        {
            var networkCredential = Credential.GetNetworkCredential();
            username = networkCredential.UserName;
            password = networkCredential.Password;
        }

        var integratedSecurity = Provider == DbaXProvider.SqlServer && string.IsNullOrEmpty(username) && string.IsNullOrEmpty(password);
        bool? ssl = MyInvocation.BoundParameters.ContainsKey(nameof(Ssl)) ? Ssl.IsPresent : null;
        var connectionString = DbaXProviderHelpers.BuildConnectionString(
            Provider,
            Server,
            Database,
            integratedSecurity,
            username,
            password,
            Port,
            ssl,
            TrustServerCertificate.IsPresent,
            ReadOnly.IsPresent,
            BusyTimeoutMs,
            ConnectTimeoutSeconds,
            ApplicationName);

        WriteObject(connectionString);
    }
}
