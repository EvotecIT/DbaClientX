namespace DBAClientX.PowerShell;

/// <summary>Executes a non-query SQL command against Oracle.</summary>
/// <para>Runs an SQL statement such as INSERT, UPDATE, or DELETE and returns the number of affected rows.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Use caution with destructive statements; the cmdlet respects <c>-WhatIf</c> and <c>-Confirm</c>.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Delete rows from a table.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXOracleNonQuery -Server 'oraclesrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'DELETE FROM Users WHERE Disabled = 1'</code>
/// <para>Removes disabled users and outputs the number of rows deleted.</para>
/// </example>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXOracleNonQuery", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXOracleNonQuery : PSCmdlet {
    internal static Func<DBAClientX.Oracle> OracleFactory { get; set; } = () => new DBAClientX.Oracle();

    /// <summary>Specifies the Oracle server.</summary>
    [Parameter(Mandatory = true)]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the target database.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>The SQL command to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter]
    public int QueryTimeout { get; set; }

    /// <summary>Provides parameters for the SQL command.</summary>
    [Parameter]
    public Hashtable? Parameters { get; set; }

    /// <summary>User name for authentication.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Username { get; set; } = string.Empty;

    /// <summary>Password for authentication.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
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
        using var oracle = OracleFactory();
        oracle.CommandTimeout = QueryTimeout;
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
            var affected = oracle.ExecuteNonQuery(Server, Database, Username, Password, Query, parameters);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXOracleNonQuery - Error executing Oracle: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}
