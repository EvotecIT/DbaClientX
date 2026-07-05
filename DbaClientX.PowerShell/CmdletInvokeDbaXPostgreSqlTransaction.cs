namespace DBAClientX.PowerShell;

/// <summary>Runs a script block inside a PostgreSQL transaction.</summary>
/// <para>Creates a PostgreSQL client, begins a transaction, invokes the script block, and commits on success.</para>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXPostgreSqlTransaction", SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.Medium)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXPostgreSqlTransaction : PSCmdlet
{
    /// <summary>Specifies the PostgreSQL server.</summary>
    [Parameter(Mandatory = true)]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the database name.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>Script block executed with the transaction client as the first argument.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public ScriptBlock ScriptBlock { get; set; } = null!;

    /// <summary>PostgreSQL user name.</summary>
    [Parameter]
    public string Username { get; set; } = string.Empty;

    /// <summary>PostgreSQL password.</summary>
    [Parameter]
    public string Password { get; set; } = string.Empty;

    /// <summary>PostgreSQL credential.</summary>
    [Parameter]
    [Credential]
    public PSCredential? Credential { get; set; }

    /// <summary>Command timeout to assign to the transaction client.</summary>
    [Parameter]
    public int QueryTimeout { get; set; }

    /// <summary>Isolation level to use for the transaction.</summary>
    [Parameter]
    public IsolationLevel IsolationLevel { get; set; } = IsolationLevel.ReadCommitted;

    /// <summary>Additional arguments passed to the script block after the transaction client.</summary>
    [Parameter]
    public object[]? ArgumentList { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        if (!ShouldProcess($"{Server}/{Database}", "Execute PostgreSQL transaction"))
        {
            return;
        }

        var credentials = PowerShellHelpers.ResolveExplicitCredential(Username, Password, Credential, "PostgreSQL");
        if (!DbaXTransactionFactoryOverrides.HasOverride(this, "PostgreSql"))
        {
            var connectionString = DBAClientX.PostgreSql.BuildConnectionString(Server, Database, credentials.Username, credentials.Password);
            if (!PowerShellHelpers.TryValidateConnection(this, "postgresql", connectionString, this.ResolveErrorAction()))
            {
                return;
            }
        }

        DbaXTransactionRunner.Execute(
            this,
            "PostgreSQL",
            () => DbaXTransactionFactoryOverrides.CreateClient(this, "PostgreSql", static () => new DBAClientX.PostgreSql()),
            client => BeginTransaction(client, credentials),
            ScriptBlock,
            ArgumentList,
            QueryTimeout,
            MyInvocation.BoundParameters.ContainsKey(nameof(QueryTimeout)));
    }

    private void BeginTransaction(object client, (string Username, string Password) credentials)
    {
        if (client is DBAClientX.PostgreSql postgreSql)
        {
            postgreSql.BeginTransaction(Server, Database, credentials.Username, credentials.Password, IsolationLevel);
            return;
        }

        DbaXTransactionRunner.InvokeBeginTransaction(client, Server, Database, credentials.Username, credentials.Password, IsolationLevel);
    }
}
