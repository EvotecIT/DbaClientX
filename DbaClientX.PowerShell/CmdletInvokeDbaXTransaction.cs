namespace DBAClientX.PowerShell;

/// <summary>Runs a script block inside a SQL Server transaction.</summary>
/// <para>Creates a SQL Server client, begins a transaction, invokes the script block, and commits on success.</para>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXTransaction", SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.Medium)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXTransaction : PSCmdlet
{
    /// <summary>Specifies the SQL Server instance.</summary>
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

    /// <summary>Optional SQL authentication user name.</summary>
    [Parameter]
    public string Username { get; set; } = string.Empty;

    /// <summary>Optional SQL authentication password.</summary>
    [Parameter]
    public string Password { get; set; } = string.Empty;

    /// <summary>Optional SQL authentication credential.</summary>
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
        if (!ShouldProcess($"{Server}/{Database}", "Execute SQL Server transaction"))
        {
            return;
        }

        var credentials = PowerShellHelpers.ResolveSqlServerCredential(Username, Password, Credential);
        if (!DbaXTransactionFactoryOverrides.HasOverride(this, "SqlServer"))
        {
            var connectionString = DBAClientX.SqlServer.BuildConnectionString(
                Server,
                Database,
                credentials.IntegratedSecurity,
                credentials.Username,
                credentials.Password);
            if (!PowerShellHelpers.TryValidateConnection(this, "sqlserver", connectionString, this.ResolveErrorAction()))
            {
                return;
            }
        }

        DbaXTransactionRunner.Execute(
            this,
            "SQL Server",
            () => DbaXTransactionFactoryOverrides.CreateClient(this, "SqlServer", static () => new DBAClientX.SqlServer()),
            client => BeginTransaction(client, credentials),
            ScriptBlock,
            ArgumentList,
            QueryTimeout,
            MyInvocation.BoundParameters.ContainsKey(nameof(QueryTimeout)));
    }

    private void BeginTransaction(object client, (string Username, string Password, bool IntegratedSecurity) credentials)
    {
        if (client is DBAClientX.SqlServer sqlServer)
        {
            sqlServer.BeginTransaction(
                Server,
                Database,
                credentials.IntegratedSecurity,
                IsolationLevel,
                credentials.Username,
                credentials.Password);
            return;
        }

        DbaXTransactionRunner.InvokeBeginTransaction(
            client,
            Server,
            Database,
            credentials.IntegratedSecurity,
            IsolationLevel,
            credentials.Username,
            credentials.Password);
    }
}
