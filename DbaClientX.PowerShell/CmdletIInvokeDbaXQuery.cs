using System.Runtime.CompilerServices;
using System.Data.Common;
using System.Data.SqlClient;

namespace DBAClientX.PowerShell;

[Cmdlet(VerbsLifecycle.Invoke, "DbaXQuery", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXQuery : AsyncPSCmdlet {
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
    protected override Task BeginProcessingAsync() {
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

        return Task.CompletedTask;
    }

    /// <summary>
    /// Process method for PowerShell cmdlet
    /// </summary>
    protected override async Task ProcessRecordAsync() {
        using var sqlServer = SqlServerFactory();
        sqlServer.ReturnType = ReturnType;
        sqlServer.CommandTimeout = QueryTimeout;
        var integratedSecurity = string.IsNullOrEmpty(Username) && string.IsNullOrEmpty(Password);
        try {
            IDictionary<string, object?>? parameters = null;
            IEnumerable<DbParameter>? dbParameters = null;
            if (Parameters != null)
            {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
                dbParameters = parameters.Select(kvp => (DbParameter)new SqlParameter(kvp.Key, kvp.Value ?? DBNull.Value)).ToList();
            }

            // Streaming branch using asynchronous enumeration when supported
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent)
            {
                var enumerable = sqlServer.QueryStreamAsync(Server, Database, integratedSecurity, Query, parameters, cancellationToken: CancelToken, username: Username, password: Password);
                switch (ReturnType)
                {
                    case ReturnType.DataRow:
                        await foreach (var row in enumerable.ConfigureAwait(false))
                        {
                            WriteObject(row);
                        }
                        break;
                    case ReturnType.DataTable:
                        DataTable? table = null;
                        await foreach (var row in enumerable.ConfigureAwait(false))
                        {
                            table ??= row.Table.Clone();
                            table.ImportRow(row);
                        }
                        if (table != null)
                        {
                            WriteObject(table);
                        }
                        break;
                    case ReturnType.DataSet:
                        DataTable? dataTable = null;
                        await foreach (var row in enumerable.ConfigureAwait(false))
                        {
                            dataTable ??= row.Table.Clone();
                            dataTable.ImportRow(row);
                        }
                        DataSet set = new DataSet();
                        if (dataTable != null)
                        {
                            set.Tables.Add(dataTable);
                        }
                        WriteObject(set);
                        break;
                    default:
                        await foreach (var row in enumerable.ConfigureAwait(false))
                        {
                            WriteObject(DataRowToPSObject(row));
                        }
                        break;
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
                result = sqlServer.ExecuteStoredProcedure(Server, Database, integratedSecurity, StoredProcedure, dbParameters, username: Username, password: Password);
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
    private static readonly ConditionalWeakTable<DataTable, PSNoteProperty[]> _psNotePropertyCache = new();

    private static PSObject DataRowToPSObject(DataRow row) {
        PSObject psObject = new PSObject();

        if (row != null && (row.RowState & DataRowState.Detached) != DataRowState.Detached) {
            var table = row.Table;
            if (!_psNotePropertyCache.TryGetValue(table, out var propertyTemplates)) {
                propertyTemplates = new PSNoteProperty[table.Columns.Count];
                for (int i = 0; i < table.Columns.Count; i++) {
                    propertyTemplates[i] = new PSNoteProperty(table.Columns[i].ColumnName, null);
                }
                _psNotePropertyCache.Add(table, propertyTemplates);
            }

            for (int i = 0; i < propertyTemplates.Length; i++) {
                var prop = (PSNoteProperty)propertyTemplates[i].Copy();
                if (!row.IsNull(i)) {
                    prop.Value = row[i];
                }
                psObject.Properties.Add(prop);
            }
        }

        return psObject;
    }
}