namespace DBAClientX.PowerShell;

[Cmdlet(VerbsLifecycle.Invoke, "DbaXQuery", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXQuery : PSCmdlet {
    internal static Func<DBAClientX.SqlServer> SqlServerFactory { get; set; } = () => new DBAClientX.SqlServer();
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string StoredProcedure { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public int QueryTimeout { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    public SwitchParameter Stream { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public Hashtable Parameters { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Username { get; set; }

    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Password { get; set; }

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
        var sqlServer = SqlServerFactory();
        sqlServer.ReturnType = ReturnType;
        sqlServer.CommandTimeout = QueryTimeout;
        var integratedSecurity = string.IsNullOrEmpty(Username) && string.IsNullOrEmpty(Password);
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null)
            {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent)
            {
                var enumerable = sqlServer.QueryStreamAsync(Server, Database, integratedSecurity, Query, parameters, cancellationToken: CancellationToken.None, username: Username, password: Password);
                var enumerator = enumerable.GetAsyncEnumerator();
                try
                {
                    switch (ReturnType)
                    {
                        case ReturnType.DataRow:
                            while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                            {
                                WriteObject(enumerator.Current);
                            }
                            break;
                        case ReturnType.DataTable:
                            DataTable? table = null;
                            while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                            {
                                table ??= enumerator.Current.Table.Clone();
                                table.ImportRow(enumerator.Current);
                            }
                            if (table != null)
                            {
                                WriteObject(table);
                            }
                            break;
                        case ReturnType.DataSet:
                            DataTable? dataTable = null;
                            while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                            {
                                dataTable ??= enumerator.Current.Table.Clone();
                                dataTable.ImportRow(enumerator.Current);
                            }
                            DataSet set = new DataSet();
                            if (dataTable != null)
                            {
                                set.Tables.Add(dataTable);
                            }
                            WriteObject(set);
                            break;
                        default:
                            while (enumerator.MoveNextAsync().AsTask().GetAwaiter().GetResult())
                            {
                                WriteObject(DataRowToPSObject(enumerator.Current));
                            }
                            break;
                    }
                }
                finally
                {
                    enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
                return;
            }
#else
            if (Stream.IsPresent)
            {
                throw new NotSupportedException("Streaming is not supported on this platform.");
            }
#endif

            object? result;
            if (!string.IsNullOrEmpty(StoredProcedure)) {
                result = sqlServer.ExecuteStoredProcedure(Server, Database, integratedSecurity, StoredProcedure, parameters, username: Username, password: Password);
            } else {
                result = sqlServer.Query(Server, Database, integratedSecurity, Query, parameters, username: Username, password: Password);
            }
            if (result != null) {
                if (ReturnType == ReturnType.PSObject) {
                    //var resultConverted = result as DataTable;
                    foreach (DataRow row in ((DataTable)result).Rows) {
                        WriteObject(DataRowToPSObject(row));
                    }
                } else {
                    WriteObject(result, true);
                }
            }
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