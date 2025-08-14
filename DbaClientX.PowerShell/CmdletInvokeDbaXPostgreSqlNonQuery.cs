namespace DBAClientX.PowerShell;

/// <summary>Executes a non-query SQL command against PostgreSQL.</summary>
/// <para>Runs statements like INSERT, UPDATE, or DELETE and returns the number of affected rows.</para>
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
/// <code>Invoke-DbaXPostgreSqlNonQuery -Server 'pgsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'DELETE FROM data WHERE Archived = 1'</code>
/// <para>Removes archived rows and outputs the number of rows deleted.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/ef/core/providers/npgsql/">Npgsql provider on MS Learn</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXPostgreSqlNonQuery", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXPostgreSqlNonQuery : PSCmdlet {
    internal static Func<DBAClientX.PostgreSql> PostgreSqlFactory { get; set; } = () => new DBAClientX.PostgreSql();

    /// <summary>Specifies the PostgreSQL server.</summary>
    [Parameter(Mandatory = true)]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; }

    /// <summary>Defines the target database.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    /// <summary>The SQL command to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter]
    public int QueryTimeout { get; set; }

    /// <summary>Provides parameters for the SQL command.</summary>
    [Parameter]
    public Hashtable Parameters { get; set; }

    /// <summary>User name for authentication.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Username { get; set; }

    /// <summary>Password for authentication.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Password { get; set; }

    private ActionPreference ErrorAction;

    protected override void BeginProcessing() {
        ErrorAction = (ActionPreference)this.SessionState.PSVariable.GetValue("ErrorActionPreference");
        if (this.MyInvocation.BoundParameters.ContainsKey("ErrorAction")) {
            string errorActionString = this.MyInvocation.BoundParameters["ErrorAction"].ToString();
            if (Enum.TryParse(errorActionString, true, out ActionPreference actionPreference)) {
                ErrorAction = actionPreference;
            }
        }
    }

    protected override void ProcessRecord() {
        using var postgreSql = PostgreSqlFactory();
        postgreSql.CommandTimeout = QueryTimeout;
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null) {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }
            var affected = postgreSql.ExecuteNonQuery(Server, Database, Username, Password, Query, parameters);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXPostgreSqlNonQuery - Error executing PostgreSql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}

