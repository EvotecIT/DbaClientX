namespace DBAClientX.PowerShell;

/// <summary>Runs a script block inside a SQLite transaction.</summary>
/// <para>Creates a SQLite client, begins a transaction, invokes the script block, and commits on success.</para>
/// <example>
/// <para>Update a row in a SQLite transaction.</para>
/// <code>Invoke-DbaXSQLiteTransaction -Database '.\app.db' -ScriptBlock { param($client) $client.ExecuteNonQuery('.\app.db', 'UPDATE Jobs SET Enabled = 1 WHERE Id = 42', $null, $true) }</code>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXSQLiteTransaction", SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.Medium)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXSQLiteTransaction : PSCmdlet
{
    /// <summary>Defines the SQLite database path or connection target.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>Script block executed with the transaction client as the first argument.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public ScriptBlock ScriptBlock { get; set; } = null!;

    /// <summary>Command timeout to assign to the transaction client. Specify 0 for no timeout.</summary>
    [Parameter]
    [ValidateRange(0, int.MaxValue)]
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
        if (!ShouldProcess(Database, "Execute SQLite transaction"))
        {
            return;
        }

        if (!DbaXTransactionFactoryOverrides.HasOverride(this, "SQLite"))
        {
            var connectionString = DBAClientX.SQLite.BuildConnectionString(Database);
            if (!PowerShellHelpers.TryValidateConnection(this, "sqlite", connectionString, this.ResolveErrorAction()))
            {
                return;
            }
        }

        DbaXTransactionRunner.Execute(
            this,
            "SQLite",
            () => DbaXTransactionFactoryOverrides.CreateClient(this, "SQLite", static () => new DBAClientX.SQLite()),
            BeginTransaction,
            ScriptBlock,
            ArgumentList,
            QueryTimeout,
            MyInvocation.BoundParameters.ContainsKey(nameof(QueryTimeout)));
    }

    private void BeginTransaction(object client)
    {
        if (client is DBAClientX.SQLite sqlite)
        {
            sqlite.BeginTransaction(Database, IsolationLevel);
            return;
        }

        DbaXTransactionRunner.InvokeBeginTransaction(client, Database, IsolationLevel);
    }
}
