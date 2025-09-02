namespace DBAClientX.PowerShell;

/// <summary>Invokes commands against an Oracle database.</summary>
/// <para>Connects to an Oracle server using provided credentials and executes a SQL query.</para>
/// <para>Results can be returned in different formats based on the <see cref="ReturnType"/>.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Streaming is not currently supported for Oracle.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Query Oracle with credentials.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXOracle -Server 'oraclesrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT * FROM Users'</code>
/// <para>Returns each row as a <see cref="DataRow"/>.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/dotnet/standard/data/sqlite/?tabs=netcore-cli">Oracle provider documentation</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXOracle", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXOracle : AsyncPSCmdlet {
    internal static Func<DBAClientX.Oracle> OracleFactory { get; set; } = () => new DBAClientX.Oracle();

    /// <summary>Specifies the Oracle server to connect to.</summary>
    [Parameter(Mandatory = true)]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; }

    /// <summary>Defines the name of the database.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    /// <summary>The SQL statement to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    /// <summary>Sets the timeout for the command in seconds.</summary>
    [Parameter]
    public int QueryTimeout { get; set; }

    /// <summary>Streams results without buffering. Not supported for Oracle.</summary>
    [Parameter]
    public SwitchParameter Stream { get; set; }

    /// <summary>Selects the format of the returned data.</summary>
    [Parameter]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    /// <summary>Provides additional parameters for the query.</summary>
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

    protected override Task BeginProcessingAsync() {
        ErrorAction = (ActionPreference)this.SessionState.PSVariable.GetValue("ErrorActionPreference");
        if (this.MyInvocation.BoundParameters.ContainsKey("ErrorAction")) {
            string errorActionString = this.MyInvocation.BoundParameters["ErrorAction"].ToString();
            if (Enum.TryParse(errorActionString, true, out ActionPreference actionPreference)) {
                ErrorAction = actionPreference;
            }
        }
        return Task.CompletedTask;
    }

    protected override async Task ProcessRecordAsync() {
        using var oracle = OracleFactory();
        oracle.ReturnType = ReturnType;
        oracle.CommandTimeout = QueryTimeout;
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null) {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }
            if (Stream.IsPresent) {
                throw new NotSupportedException("Streaming is not supported for Oracle.");
            }
            var result = await oracle.QueryAsync(Server, Database, Username, Password, Query, parameters, cancellationToken: CancelToken).ConfigureAwait(false);
            if (result != null) {
                if (ReturnType == ReturnType.PSObject) {
                    foreach (DataRow row in ((DataTable)result).Rows) {
                        WriteObject(PSObjectConverter.DataRowToPSObject(row));
                    }
                } else {
                    WriteObject(result, true);
                }
            }
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXOracle - Error querying Oracle: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}
