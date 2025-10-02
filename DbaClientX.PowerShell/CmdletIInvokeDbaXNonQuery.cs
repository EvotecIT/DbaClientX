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
/// <summary>Delete rows from a table.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXNonQuery -Server 'sqlsrv' -Database 'app' -Query 'DELETE FROM Users WHERE Disabled = 1'</code>
/// <para>Removes disabled users and outputs the number of rows deleted.</para>
/// </example>
/// <example>
/// <summary>Run a command with SQL authentication.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXNonQuery -Server 'sqlsrv' -Database 'app' -Query 'TRUNCATE TABLE Logs' -Username 'user' -Password 'p@ss'</code>
/// <para>Executes the statement using the supplied credentials.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/dotnet/framework/data/adonet/using-sqlclient">Using SqlClient</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXNonQuery", DefaultParameterSetName = "DefaultCredentials", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXNonQuery : PSCmdlet {
    internal static Func<DBAClientX.SqlServer> SqlServerFactory { get; set; } = () => new DBAClientX.SqlServer();
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
        using var sqlServer = SqlServerFactory();
        sqlServer.CommandTimeout = QueryTimeout;
        var integratedSecurity = string.IsNullOrEmpty(Username) && string.IsNullOrEmpty(Password);
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);

            var affected = sqlServer.ExecuteNonQuery(Server, Database, integratedSecurity, Query, parameters, username: Username, password: Password);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXNonQuery - Error querying SqlServer: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}
