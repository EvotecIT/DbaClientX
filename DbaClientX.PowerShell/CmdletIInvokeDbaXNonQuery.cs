namespace DBAClientX.PowerShell;

/// <summary>Executes a non-query SQL command against SQL Server.</summary>
/// <para>Runs an SQL statement such as INSERT, UPDATE, or DELETE and returns the number of affected rows.</para>
/// <para>Supports SQL authentication or integrated security based on provided credentials.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Use caution with destructive statements; the cmdlet respects <c>-WhatIf</c> and <c>-Confirm</c>.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Create and populate a local temporary table.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$sql = @'
/// CREATE TABLE #DbaClientXDemo
/// (
///     Id int NOT NULL,
///     Name nvarchar(50) NOT NULL
/// );
///
/// INSERT INTO #DbaClientXDemo (Id, Name)
/// VALUES (1, N'Alpha'), (2, N'Beta');
/// '@
///
/// Invoke-DbaXNonQuery -Server 'localhost' -Database 'master' -TrustServerCertificate -Query $sql</code>
/// <para>Executes a multi-line command and returns the number of affected rows.</para>
/// </example>
/// <example>
/// <summary>Run a command with SQL authentication.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$credential = Get-Credential 'app_writer'
/// Invoke-DbaXNonQuery -Server 'sql01' -Database 'app' -Query 'UPDATE dbo.Users SET LastSeenUtc = SYSUTCDATETIME() WHERE Id = @Id' -Credential $credential -Parameters @{ Id = 42 }</code>
/// <para>Executes the statement using the supplied credentials.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/dotnet/framework/data/adonet/using-sqlclient">Using SqlClient</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXNonQuery", DefaultParameterSetName = "DefaultCredentials", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXNonQuery : PSCmdlet {
    internal static Func<DBAClientX.SqlServer> SqlServerFactory { get; set; } = () => new DBAClientX.SqlServer();
    internal static ScriptBlock? NonQueryOverride { get; set; }
    /// <summary>Specifies the SQL Server instance.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "DefaultCredentials")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the target database.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "DefaultCredentials")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>The SQL command to execute.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "DefaultCredentials")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public int QueryTimeout { get; set; }

    /// <summary>Provides parameters for the SQL command.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public Hashtable? Parameters { get; set; }

    /// <summary>Optional user name for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public string Username { get; set; } = string.Empty;

    /// <summary>Optional password for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public string Password { get; set; } = string.Empty;

    /// <summary>Optional SQL authentication credential.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    [Credential]
    public PSCredential? Credential { get; set; }

    /// <summary>Trusts the SQL Server TLS certificate without validating the certificate chain.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public SwitchParameter TrustServerCertificate { get; set; }

    private ActionPreference ErrorAction;

    /// <summary>
    /// Initializes cmdlet state before pipeline execution begins.
    /// </summary>
    protected override void BeginProcessing() {
        ErrorAction = this.ResolveErrorAction();
    }

    /// <summary>
    /// Processes input and performs the cmdlet's primary work.
    /// </summary>
    protected override void ProcessRecord() {
        if (!ShouldProcess($"{Server}/{Database}", "Execute SQL Server non-query")) {
            return;
        }
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
            if (NonQueryOverride is not null)
            {
                var result = PowerShellHelpers.InvokeOverrideAsync<object?>(NonQueryOverride, this, parameters)
                    .ConfigureAwait(false)
                    .GetAwaiter()
                    .GetResult();
                if (result != null)
                {
                    WriteObject(result);
                }
                return;
            }

            var (resolvedUsername, resolvedPassword, integratedSecurity) = PowerShellHelpers.ResolveSqlServerCredential(Username, Password, Credential);
            var connectionString = DBAClientX.SqlServer.BuildConnectionString(
                Server,
                Database,
                integratedSecurity,
                resolvedUsername,
                resolvedPassword,
                trustServerCertificate: TrustServerCertificate.IsPresent);
            if (!PowerShellHelpers.TryValidateConnection(this, "sqlserver", connectionString, ErrorAction))
            {
                return;
            }

            using var sqlServer = CreateSqlServer();
            var affected = sqlServer.ExecuteNonQuery(connectionString, Query, parameters);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXNonQuery - Error querying SqlServer: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

    private DBAClientX.SqlServer CreateSqlServer()
    {
        var sqlServer = SqlServerFactory();
        sqlServer.CommandTimeout = QueryTimeout;
        return sqlServer;
    }
}
