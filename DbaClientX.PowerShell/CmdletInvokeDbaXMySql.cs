using System.Runtime.CompilerServices;

namespace DBAClientX.PowerShell;

[Cmdlet(VerbsLifecycle.Invoke, "DbaXMySql", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXMySql : AsyncPSCmdlet {
    internal static Func<DBAClientX.MySql> MySqlFactory { get; set; } = () => new DBAClientX.MySql();

    [Parameter(Mandatory = true)]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; }

    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    [Parameter]
    public int QueryTimeout { get; set; }

    [Parameter]
    public SwitchParameter Stream { get; set; }

    [Parameter]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    [Parameter]
    public Hashtable Parameters { get; set; }

    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Username { get; set; }

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
        using var mySql = MySqlFactory();
        mySql.ReturnType = ReturnType;
        mySql.CommandTimeout = QueryTimeout;
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null) {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent) {
                var enumerable = mySql.QueryStreamAsync(Server, Database, Username, Password, Query, parameters, cancellationToken: CancelToken);
                switch (ReturnType) {
                    case ReturnType.DataRow:
                        await foreach (var row in enumerable.ConfigureAwait(false)) {
                            WriteObject(row);
                        }
                        break;
                    case ReturnType.DataTable:
                        DataTable? table = null;
                        await foreach (var row in enumerable.ConfigureAwait(false)) {
                            table ??= row.Table.Clone();
                            table.ImportRow(row);
                        }
                        if (table != null) {
                            WriteObject(table);
                        }
                        break;
                    case ReturnType.DataSet:
                        DataTable? dataTable = null;
                        await foreach (var row in enumerable.ConfigureAwait(false)) {
                            dataTable ??= row.Table.Clone();
                            dataTable.ImportRow(row);
                        }
                        DataSet set = new DataSet();
                        if (dataTable != null) {
                            set.Tables.Add(dataTable);
                        }
                        WriteObject(set);
                        break;
                    default:
                        await foreach (var row in enumerable.ConfigureAwait(false)) {
                            WriteObject(DataRowToPSObject(row));
                        }
                        break;
                }
                return;
            }
#else
            if (Stream.IsPresent) {
                throw new NotSupportedException("Streaming is not supported on this platform.");
            }
#endif
            var result = await mySql.QueryAsync(Server, Database, Username, Password, Query, parameters, cancellationToken: CancelToken).ConfigureAwait(false);
            if (result != null) {
                if (ReturnType == ReturnType.PSObject) {
                    foreach (DataRow row in ((DataTable)result).Rows) {
                        WriteObject(DataRowToPSObject(row));
                    }
                } else {
                    WriteObject(result, true);
                }
            }
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXMySql - Error querying MySql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

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
