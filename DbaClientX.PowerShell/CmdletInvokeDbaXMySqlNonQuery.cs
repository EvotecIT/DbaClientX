namespace DBAClientX.PowerShell;

/// <summary>Executes a non-query SQL command against MySQL.</summary>
/// <para>Runs an SQL statement such as INSERT, UPDATE, or DELETE and returns the number of affected rows.</para>
/// <para>Requires explicit credentials for authentication.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Use caution with destructive statements; the cmdlet respects <c>-WhatIf</c> and <c>-Confirm</c>.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Delete rows from a table.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXMySqlNonQuery -Server 'mysqlsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'DELETE FROM Users WHERE Disabled = 1'</code>
/// <para>Removes disabled users and outputs the number of rows deleted.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/ef/core/providers/?tabs=dotnet-core-cli#mysql">MySQL provider on MS Learn</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXMySqlNonQuery", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXMySqlNonQuery : PSCmdlet {
    internal static Func<DBAClientX.MySql> MySqlFactory { get; set; } = () => new DBAClientX.MySql();

    /// <summary>Specifies the MySQL server.</summary>
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
        using var mySql = MySqlFactory();
        mySql.CommandTimeout = QueryTimeout;
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
            var affected = mySql.ExecuteNonQuery(Server, Database, Username, Password, Query, parameters);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXMySqlNonQuery - Error executing MySql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}

