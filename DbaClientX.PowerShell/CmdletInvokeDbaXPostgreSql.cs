using System.Runtime.CompilerServices;
using System.Data.Common;
using Npgsql;

namespace DBAClientX.PowerShell;

/// <summary>Invokes commands against a PostgreSQL database.</summary>
/// <para>Connects to a PostgreSQL server and executes a query or stored procedure with optional parameters.</para>
/// <para>Results can be streamed or returned in DataRow, DataTable, DataSet or PSObject formats.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Credentials are transmitted to the server; ensure secure channels when running over a network.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Run a query using explicit credentials.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXPostgreSql -Server 'pgsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT * FROM data'</code>
/// <para>Executes the query and returns each row as a <see cref="DataRow"/>.</para>
/// </example>
/// <example>
/// <summary>Execute a stored procedure and get a DataTable.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXPostgreSql -Server 'pgsrv' -Database 'app' -Username 'user' -Password 'p@ss' -StoredProcedure 'refresh_stats' -ReturnType DataTable</code>
/// <para>Runs the stored procedure and outputs a <see cref="DataTable"/>.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/ef/core/providers/npgsql/">Npgsql provider on MS Learn</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXPostgreSql", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXPostgreSql : AsyncPSCmdlet {
    internal static Func<DBAClientX.PostgreSql> PostgreSqlFactory { get; set; } = () => new DBAClientX.PostgreSql();

    /// <summary>Specifies the PostgreSQL server to connect to.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; }

    /// <summary>Defines the database name on the server.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    /// <summary>Provides the SQL query text to execute.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    /// <summary>Names the stored procedure to invoke.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string StoredProcedure { get; set; }

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter(ParameterSetName = "Query")]
    [Parameter(ParameterSetName = "StoredProcedure")]
    public int QueryTimeout { get; set; }

    /// <summary>Streams results instead of buffering them.</summary>
    [Parameter(ParameterSetName = "Query")]
    public SwitchParameter Stream { get; set; }

    /// <summary>Selects the format of returned data.</summary>
    [Parameter(ParameterSetName = "Query")]
    [Parameter(ParameterSetName = "StoredProcedure")]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    /// <summary>Supplies parameters for the query or stored procedure.</summary>
    [Parameter(ParameterSetName = "Query")]
    [Parameter(ParameterSetName = "StoredProcedure")]
    public Hashtable Parameters { get; set; }

    /// <summary>The user name for authentication.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Username { get; set; }

    /// <summary>The password for authentication.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
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
        using var postgreSql = PostgreSqlFactory();
        postgreSql.ReturnType = ReturnType;
        postgreSql.CommandTimeout = QueryTimeout;
        try {
            IDictionary<string, object?>? parameters = null;
            IEnumerable<DbParameter>? dbParameters = null;
            if (Parameters != null) {
                parameters = Parameters.Cast<DictionaryEntry>().ToDictionary(
                    de => de.Key.ToString(),
                    de => de.Value);
                dbParameters = parameters.Select(kvp => (DbParameter)new NpgsqlParameter(kvp.Key, kvp.Value ?? DBNull.Value)).ToList();
            }
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent) {
                var enumerable = postgreSql.QueryStreamAsync(Server, Database, Username, Password, Query, parameters, cancellationToken: CancelToken);
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
            object? result;
            if (!string.IsNullOrEmpty(StoredProcedure)) {
                result = postgreSql.ExecuteStoredProcedure(Server, Database, Username, Password, StoredProcedure, dbParameters);
            } else {
                result = postgreSql.Query(Server, Database, Username, Password, Query, parameters);
            }
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
            WriteWarning($"Invoke-DbaXPostgreSql - Error querying PostgreSql: {ex.Message}");
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
