namespace DBAClientX.PowerShell;

[Cmdlet(VerbsLifecycle.Invoke, "DbaXNonQuery", DefaultParameterSetName = "DefaultCredentials", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXNonQuery : PSCmdlet {
    [Parameter(Mandatory = true, ParameterSetName = "DefaultCredentials")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    public string Server { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = "DefaultCredentials")]
    public string Database { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = "DefaultCredentials")]
    public string Query { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public int QueryTimeout { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public Hashtable Parameters { get; set; }

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
        var sqlServer = new DBAClientX.SqlServer {
            CommandTimeout = QueryTimeout
        };
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null) {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }

            var affected = sqlServer.SqlQueryNonQuery(Server, Database, true, Query, parameters);
            WriteObject(affected);
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXNonQuery - Error querying SqlServer: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}
