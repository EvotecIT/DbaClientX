namespace DBAClientX.PowerShell;

[Cmdlet(VerbsLifecycle.Invoke, "DbaXQuery", DefaultParameterSetName = "Compatibility", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXQuery : PSCmdlet {
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
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public Hashtable Parameters { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "DefaultCredentials")]
    public SwitchParameter Stream { get; set; }

    // allows tests to provide a custom SqlServer implementation
    public static DBAClientX.SqlServer? SqlServerOverride { get; set; }

    // allows tests to override streaming source
    public static Func<IAsyncEnumerable<DataRow>>? StreamGenerator { get; set; }

    private ActionPreference ErrorAction;

    /// <summary>
    /// Begin processing method for PowerShell cmdlet
    /// </summary>
    protected override void BeginProcessing() {
        // Get the error action preference as user requested
        // It first sets the error action to the default error action preference
        // If the user has specified the error action, it will set the error action to the user specified error action
        ErrorAction = (ActionPreference)this.SessionState.PSVariable.GetValue("ErrorActionPreference");
        if (this.MyInvocation.BoundParameters.ContainsKey("ErrorAction")) {
            string errorActionString = this.MyInvocation.BoundParameters["ErrorAction"].ToString();
            if (Enum.TryParse(errorActionString, true, out ActionPreference actionPreference)) {
                ErrorAction = actionPreference;
            }
        }
    }

    /// <summary>
    /// Process method for PowerShell cmdlet
    /// </summary>
    protected override void ProcessRecord() {
        var sqlServer = SqlServerOverride ?? new DBAClientX.SqlServer();
        sqlServer.ReturnType = ReturnType;
        sqlServer.CommandTimeout = QueryTimeout;
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null)
            {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent) {
                var stream = StreamGenerator != null ? StreamGenerator() : sqlServer.SqlQueryStreamAsync(Server, Database, true, Query, parameters);
                var enumerator = stream.GetAsyncEnumerator();
                try {
                    while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult()) {
                        WriteObject(DataRowToPSObject(enumerator.Current));
                    }
                } finally {
                    enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            } else {
                var result = sqlServer.SqlQuery(Server, Database, true, Query, parameters);
                if (result != null) {
                    if (ReturnType == ReturnType.PSObject) {
                        foreach (DataRow row in ((DataTable)result).Rows) {
                            WriteObject(DataRowToPSObject(row));
                        }
                    } else {
                        WriteObject(result, true);
                    }
                }
            }
#else
            if (Stream.IsPresent) {
                throw new NotSupportedException("Stream parameter requires .NET Standard 2.1 or .NET Core 3.0 or higher.");
            }
            var result = sqlServer.SqlQuery(Server, Database, true, Query, parameters);
            if (result != null) {
                if (ReturnType == ReturnType.PSObject) {
                    foreach (DataRow row in ((DataTable)result).Rows) {
                        WriteObject(DataRowToPSObject(row));
                    }
                } else {
                    WriteObject(result, true);
                }
            }
#endif
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXQuery - Error querying SqlServer: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

    /// <summary>
    /// Convert DataRow to PSObject
    /// </summary>
    /// <param name="row"></param>
    /// <returns></returns>
    private static PSObject DataRowToPSObject(DataRow row) {
        PSObject psObject = new PSObject();

        if (row != null && (row.RowState & DataRowState.Detached) != DataRowState.Detached) {
            foreach (DataColumn column in row.Table.Columns) {
                Object value = null;
                if (!row.IsNull(column)) {
                    value = row[column];
                }

                psObject.Properties.Add(new PSNoteProperty(column.ColumnName, value));
            }
        }

        return psObject;
    }
}