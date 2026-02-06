
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
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the name of the database.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>The SQL statement to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

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
    public Hashtable? Parameters { get; set; }

    /// <summary>User name for authentication.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Username { get; set; } = string.Empty;

    /// <summary>Password for authentication.</summary>
    [Parameter(Mandatory = true)]
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
        using var mySql = MySqlFactory();
        mySql.ReturnType = ReturnType;
        mySql.CommandTimeout = QueryTimeout;
        var connectionString = DBAClientX.MySql.BuildConnectionString(Server, Database, Username, Password);
        if (!PowerShellHelpers.TryValidateConnection(this, "mysql", connectionString, ErrorAction))
        {
            return;
        }
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
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
            var result = await mySql.QueryAsync(Server, Database, Username, Password, Query, parameters, cancellationToken: CancelToken).ConfigureAwait(false);
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
            WriteWarning($"Invoke-DbaXMySql - Error querying MySql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

}
