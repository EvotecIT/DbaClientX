using System.Runtime.CompilerServices;

namespace DBAClientX.PowerShell;

/// <summary>Invokes commands against a MySQL database.</summary>
/// <para>Connects to a MySQL server using provided credentials and executes a SQL query.</para>
/// <para>Results can be streamed or returned in different formats based on the <see cref="ReturnType"/>.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Network operations may incur latency; consider using <c>-Stream</c> for large result sets.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Query MySQL with credentials.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXMySql -Server 'mysqlsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT * FROM Users'</code>
/// <para>Returns each row as a <see cref="DataRow"/>.</para>
/// </example>
/// <example>
/// <summary>Stream results as they arrive.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXMySql -Server 'mysqlsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT * FROM Logs' -Stream</code>
/// <para>Streams rows without buffering the entire result.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/ef/core/providers/?tabs=dotnet-core-cli#mysql">MySQL provider on MS Learn</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXMySql", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXMySql : AsyncPSCmdlet {
    internal static Func<DBAClientX.MySql> MySqlFactory { get; set; } = () => new DBAClientX.MySql();

    /// <summary>Specifies the MySQL server to connect to.</summary>
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

    /// <summary>Streams results without buffering.</summary>
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
