
namespace DBAClientX.PowerShell;

/// <summary>Invokes a query against a SQLite database.</summary>
/// <para>Executes SQL statements on a specified SQLite database and returns data in the format you choose.</para>
/// <para>Supports streaming results for large data sets when the platform allows asynchronous enumeration.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>When <c>-Stream</c> is used on platforms without streaming support, a <see cref="NotSupportedException"/> is thrown.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Run a query and return rows.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXSQLite -Database 'app.db' -Query 'SELECT * FROM Users'</code>
/// <para>Executes the query and outputs each row as a <see cref="DataRow"/>.</para>
/// </example>
/// <example>
/// <summary>Stream results from a large query.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXSQLite -Database 'app.db' -Query 'SELECT * FROM Logs' -Stream -ReturnType DataRow</code>
/// <para>Streams each row as it is received, which is useful for large result sets.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/dotnet/standard/data/sqlite/">SQLite in .NET</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXSQLite", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXSQLite : AsyncPSCmdlet {
    internal static Func<DBAClientX.SQLite> SQLiteFactory { get; set; } = () => new DBAClientX.SQLite();

    /// <summary>Specifies the path to the SQLite database file.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    /// <summary>Defines the SQL query to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter]
    public int QueryTimeout { get; set; }

    /// <summary>Streams results instead of buffering them.</summary>
    [Parameter]
    public SwitchParameter Stream { get; set; }

    /// <summary>Selects the format of returned data.</summary>
    [Parameter]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    /// <summary>Provides additional query parameters.</summary>
    [Parameter]
    public Hashtable Parameters { get; set; }

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
        using var sqlite = SQLiteFactory();
        sqlite.ReturnType = ReturnType;
        sqlite.CommandTimeout = QueryTimeout;
        try {
            IDictionary<string, object?>? parameters = null;
            if (Parameters != null) {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
            }
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent) {
                var enumerable = sqlite.QueryStreamAsync(Database, Query, parameters, cancellationToken: CancelToken);
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
                            WriteObject(PSObjectConverter.DataRowToPSObject(row));
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
            var result = sqlite.Query(Database, Query, parameters);
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
            WriteWarning($"Invoke-DbaXSQLite - Error querying SQLite: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

}
