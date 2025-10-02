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
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the database name on the server.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>Provides the SQL query text to execute.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Names the stored procedure to invoke.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string StoredProcedure { get; set; } = string.Empty;

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
    public Hashtable? Parameters { get; set; }

    /// <summary>The user name for authentication.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Username { get; set; } = string.Empty;

    /// <summary>The password for authentication.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Password { get; set; } = string.Empty;

    private ActionPreference ErrorAction;

    /// <summary>
    /// Initializes cmdlet state before pipeline execution begins.
    /// </summary>
    protected override Task BeginProcessingAsync() {
        ErrorAction = this.ResolveErrorAction();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Processes input and performs the cmdlet's primary work.
    /// </summary>
    protected override async Task ProcessRecordAsync() {
        await Task.Yield();
        using var postgreSql = PostgreSqlFactory();
        postgreSql.ReturnType = ReturnType;
        postgreSql.CommandTimeout = QueryTimeout;
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
            IEnumerable<DbParameter>? dbParameters = parameters?.Select(kvp => (DbParameter)new NpgsqlParameter(kvp.Key, kvp.Value ?? DBNull.Value)).ToList();
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
            object? result;
            if (!string.IsNullOrEmpty(StoredProcedure)) {
                result = postgreSql.ExecuteStoredProcedure(Server, Database, Username, Password, StoredProcedure, dbParameters);
            } else {
                result = postgreSql.Query(Server, Database, Username, Password, Query, parameters);
            }
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
            WriteWarning($"Invoke-DbaXPostgreSql - Error querying PostgreSql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

}
