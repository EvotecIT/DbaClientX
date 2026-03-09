using System.Data.Common;
using Microsoft.Data.SqlClient;

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
    internal static ScriptBlock? QueryOverride { get; set; }
    internal static ScriptBlock? QueryStreamOverride { get; set; }
    internal static ScriptBlock? StoredProcedureOverride { get; set; }
    internal static ScriptBlock? StoredProcedureStreamOverride { get; set; }

    /// <summary>Specifies the SQL Server instance.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the database name.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>The SQL statement to execute.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Name of the stored procedure to run.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string StoredProcedure { get; set; } = string.Empty;

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
    public Hashtable? Parameters { get; set; }

    /// <summary>Optional user name for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Username { get; set; } = string.Empty;

    /// <summary>Optional password for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Password { get; set; } = string.Empty;

    private ActionPreference ErrorAction;

    /// <summary>
    /// Initializes cmdlet state before pipeline execution begins.
    /// </summary>
    protected override Task BeginProcessingAsync() {
        // Get the error action preference as user requested
        // It first sets the error action to the default error action preference
        // If the user has specified the error action, it will set the error action to the user specified error action
        ErrorAction = this.ResolveErrorAction();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Processes input and performs the cmdlet's primary work.
    /// </summary>
    protected override async Task ProcessRecordAsync() {
        await Task.Yield();
        var action = !string.IsNullOrEmpty(StoredProcedure) ? "Execute SQL Server stored procedure" : "Execute SQL Server query";
        if (!ShouldProcess($"{Server}/{Database}", action)) {
            return;
        }
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);

            // Streaming branch using asynchronous enumeration when supported
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent)
            {
                if (!string.IsNullOrEmpty(StoredProcedure))
                {
                    if (StoredProcedureStreamOverride is not null)
                    {
                        var overrideParameters = PowerShellHelpers.ToInMemoryDbParameters(parameters);
                        WriteRows(PowerShellHelpers.InvokeDataRowOverride(StoredProcedureStreamOverride, this, parameters, overrideParameters));
                        return;
                    }
                }

                if (QueryStreamOverride is not null)
                {
                    WriteRows(PowerShellHelpers.InvokeDataRowOverride(QueryStreamOverride, this, parameters, null));
                    return;
                }
            }
#else
            if (Stream.IsPresent)
            {
                throw new NotSupportedException("Streaming is not supported on this platform.");
            }
#endif

            object? result;
            if (!string.IsNullOrEmpty(StoredProcedure)) {
                if (StoredProcedureOverride is not null)
                {
                    var overrideParameters = PowerShellHelpers.ToInMemoryDbParameters(parameters);
                    result = await PowerShellHelpers.InvokeOverrideAsync<object?>(StoredProcedureOverride, this, parameters, overrideParameters).ConfigureAwait(false);
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
                    return;
                }
            } else {
                if (QueryOverride is not null)
                {
                    result = await PowerShellHelpers.InvokeOverrideAsync<object?>(QueryOverride, this, parameters, null).ConfigureAwait(false);
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
                    return;
                }
            }

            var integratedSecurity = string.IsNullOrEmpty(Username) && string.IsNullOrEmpty(Password);
            var connectionString = DBAClientX.SqlServer.BuildConnectionString(Server, Database, integratedSecurity, Username, Password);
            if (!PowerShellHelpers.TryValidateConnection(this, "sqlserver", connectionString, ErrorAction))
            {
                return;
            }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent)
            {
                if (!string.IsNullOrEmpty(StoredProcedure))
                {
                    var dbParameters = PowerShellHelpers.ToDbParameters(parameters, static (name, value) => (DbParameter)new SqlParameter(name, value ?? DBNull.Value));
                    using var sqlServer = CreateSqlServer();
                    await WriteRowsAsync(sqlServer.ExecuteStoredProcedureStreamAsync(Server, Database, integratedSecurity, StoredProcedure, dbParameters, cancellationToken: CancelToken, username: Username, password: Password)).ConfigureAwait(false);
                    return;
                }

                using var streamSqlServer = CreateSqlServer();
                await WriteRowsAsync(streamSqlServer.QueryStreamAsync(Server, Database, integratedSecurity, Query, parameters, cancellationToken: CancelToken, username: Username, password: Password)).ConfigureAwait(false);
                return;
            }
#endif

            if (!string.IsNullOrEmpty(StoredProcedure)) {
                var dbParameters = PowerShellHelpers.ToDbParameters(parameters, static (name, value) => (DbParameter)new SqlParameter(name, value ?? DBNull.Value));
                using var sqlServer = CreateSqlServer();
                result = sqlServer.ExecuteStoredProcedure(Server, Database, integratedSecurity, StoredProcedure, dbParameters, username: Username, password: Password);
            } else {
                using var sqlServer = CreateSqlServer();
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

    private DBAClientX.SqlServer CreateSqlServer()
    {
        var sqlServer = SqlServerFactory();
        sqlServer.ReturnType = ReturnType;
        sqlServer.CommandTimeout = QueryTimeout;
        return sqlServer;
    }

    private void WriteRows(IEnumerable<DataRow> rows)
    {
        switch (ReturnType)
        {
            case ReturnType.DataRow:
                foreach (var row in rows)
                {
                    WriteObject(row);
                }
                break;
            case ReturnType.DataTable:
                DataTable? table = null;
                foreach (var row in rows)
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
                foreach (var row in rows)
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
                foreach (var row in rows)
                {
                    WriteObject(PSObjectConverter.DataRowToPSObject(row));
                }
                break;
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    private async Task WriteRowsAsync(IAsyncEnumerable<DataRow> rows)
    {
        switch (ReturnType)
        {
            case ReturnType.DataRow:
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    WriteObject(row);
                }
                break;
            case ReturnType.DataTable:
                DataTable? table = null;
                await foreach (var row in rows.ConfigureAwait(false))
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
                await foreach (var row in rows.ConfigureAwait(false))
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
                await foreach (var row in rows.ConfigureAwait(false))
                {
                    WriteObject(PSObjectConverter.DataRowToPSObject(row));
                }
                break;
        }
    }
#endif
}
