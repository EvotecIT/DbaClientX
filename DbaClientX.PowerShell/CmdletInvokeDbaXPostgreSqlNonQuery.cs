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
    internal static ScriptBlock? NonQueryOverride { get; set; }

    /// <summary>Specifies the PostgreSQL server.</summary>
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
    [Parameter]
    public string Username { get; set; } = string.Empty;

    /// <summary>Password for authentication.</summary>
    [Parameter]
    public string Password { get; set; } = string.Empty;

    /// <summary>Credential for authentication.</summary>
    [Parameter]
    [Credential]
    public PSCredential? Credential { get; set; }

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
        using var postgreSql = PostgreSqlFactory();
        postgreSql.CommandTimeout = QueryTimeout;
        if (!ShouldProcess($"{Server}/{Database}", "Execute PostgreSQL non-query")) {
            return;
        }
        var (resolvedUsername, resolvedPassword) = PowerShellHelpers.ResolveExplicitCredential(Username, Password, Credential, "PostgreSQL");
        var connectionString = DBAClientX.PostgreSql.BuildConnectionString(Server, Database, resolvedUsername, resolvedPassword);
        if (!PowerShellHelpers.TryValidateConnection(this, "postgresql", connectionString, ErrorAction))
        {
            return;
        }
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
            var affected = NonQueryOverride is not null
                ? PowerShellHelpers.InvokeOverrideAsync<int>(NonQueryOverride, this, parameters, resolvedUsername, resolvedPassword).GetAwaiter().GetResult()
                : postgreSql.ExecuteNonQuery(Server, Database, resolvedUsername, resolvedPassword, Query, parameters);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXPostgreSqlNonQuery - Error executing PostgreSql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}
