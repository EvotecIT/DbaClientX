using System.Data.Common;
using System.Data.SqlClient;

namespace DBAClientX.PowerShell;

/// <summary>Invokes a SQL Server query or stored procedure.</summary>
/// <para>Connects to a SQL Server instance using integrated security or supplied credentials and executes the specified command.</para>
/// <para>Supports streaming results and multiple return formats via the <see cref="ReturnType"/> parameter.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>When <c>-ErrorAction Stop</c> is used, execution will terminate on the first error.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Run a query with integrated security.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXQuery -Server 'sqlsrv' -Database 'app' -Query 'SELECT * FROM Users'</code>
/// <para>Executes the query and returns each row as a <see cref="DataRow"/>.</para>
/// </example>
/// <example>
/// <summary>Execute a stored procedure using credentials.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXQuery -Server 'sqlsrv' -Database 'app' -StoredProcedure 'usp_DoWork' -Username 'user' -Password 'p@ss' -ReturnType DataTable</code>
/// <para>Runs the stored procedure and outputs a <see cref="DataTable"/>.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/dotnet/framework/data/adonet/using-sqlclient">Using SqlClient</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXQuery", DefaultParameterSetName = "Query", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletIInvokeDbaXQuery : AsyncPSCmdlet {
    internal static Func<DBAClientX.SqlServer> SqlServerFactory { get; set; } = () => new DBAClientX.SqlServer();

    /// <summary>Specifies the SQL Server instance.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; }

    /// <summary>Defines the database name.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; }

    /// <summary>The SQL statement to execute.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; }

    /// <summary>Name of the stored procedure to run.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string StoredProcedure { get; set; }

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public int QueryTimeout { get; set; }

    /// <summary>Streams results instead of buffering them.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public SwitchParameter Stream { get; set; }

    /// <summary>Selects the type of returned objects.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    /// <summary>Provides additional parameters for the query or procedure.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public Hashtable Parameters { get; set; }

    /// <summary>Optional user name for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Username { get; set; }

    /// <summary>Optional password for SQL authentication.</summary>
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
                IAsyncEnumerable<DataRow> enumerable;
                if (!string.IsNullOrEmpty(StoredProcedure))
                {
                    enumerable = sqlServer.ExecuteStoredProcedureStreamAsync(Server, Database, integratedSecurity, StoredProcedure, dbParameters, cancellationToken: CancelToken, username: Username, password: Password);
                }
                else
                {
                    enumerable = sqlServer.QueryStreamAsync(Server, Database, integratedSecurity, Query, parameters, cancellationToken: CancelToken, username: Username, password: Password);
                }
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
                            WriteObject(PSObjectConverter.DataRowToPSObject(row));
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
                        WriteObject(PSObjectConverter.DataRowToPSObject(row));
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

}
