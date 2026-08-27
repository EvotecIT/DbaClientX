using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace DBAClientX.PowerShell;

/// <summary>Invokes a SQL Server query or stored procedure.</summary>
/// <para>Connects to a SQL Server instance using integrated security or supplied credentials and executes the specified command.</para>
/// <para>Supports streaming results, multiple buffered return formats, and transferring an owned data reader to a consuming API.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>When <c>-ErrorAction Stop</c> is used, execution will terminate on the first error.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Run a query with integrated security.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$rows = Invoke-DbaXQuery -Server 'localhost' -Database 'master' -TrustServerCertificate -Query @'
/// SELECT
///     name,
///     database_id,
///     create_date
/// FROM sys.databases
/// WHERE database_id &gt; 4
/// ORDER BY name;
/// '@
///
/// $rows | Format-Table name, database_id, create_date</code>
/// <para>Executes a multi-line query against a local SQL Server instance and returns every row as a PowerShell object.</para>
/// </example>
/// <example>
/// <summary>Execute a stored procedure using credentials.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$credential = Get-Credential 'app_reader'
/// Invoke-DbaXQuery -Server 'sql01' -Database 'app' -StoredProcedure 'dbo.usp_GetActiveUsers' -Credential $credential -ReturnType DataTable</code>
/// <para>Runs the stored procedure and outputs a <see cref="DataTable"/>.</para>
/// </example>
/// <example>
/// <summary>Stream query rows into a file writer.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$reader = Invoke-DbaXQuery -Server 'sql01' -Database 'app' -Query 'SELECT * FROM dbo.Users' -AsDataReader
/// try {
///     Export-OfficeCsv -InputObject $reader -Path .\Users.csv
/// } finally {
///     $reader.Dispose()
/// }</code>
/// <para>Returns one live <see cref="DbDataReader"/>. The caller must dispose it after the consuming API finishes reading.</para>
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
    [Parameter(Mandatory = true, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the database name.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>The SQL statement to execute.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "QueryReader")]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Name of the stored procedure to run.</summary>
    [Parameter(Mandatory = true, ParameterSetName = "StoredProcedure")]
    [ValidateNotNullOrEmpty]
    public string StoredProcedure { get; set; } = string.Empty;

    /// <summary>Sets the command timeout in seconds. Specify 0 for no timeout.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    [ValidateRange(0, int.MaxValue)]
    public int QueryTimeout { get; set; }

    /// <summary>Streams results instead of buffering them.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public SwitchParameter Stream { get; set; }

    /// <summary>Selects the type of returned objects. Defaults to PSObject so an ordinary PowerShell query emits every row.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.PSObject;

    /// <summary>
    /// When enabled, returns one live reader that owns its command and connection until the caller disposes it.
    /// A disabled switch remains compatible with ordinary buffered query options.
    /// </summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = true, ParameterSetName = "QueryReader")]
    public SwitchParameter AsDataReader { get; set; }

    /// <summary>Provides additional parameters for the query or procedure.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public Hashtable? Parameters { get; set; }

    /// <summary>Optional user name for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Username { get; set; } = string.Empty;

    /// <summary>Optional password for SQL authentication.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public string Password { get; set; } = string.Empty;

    /// <summary>Optional SQL authentication credential.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    [Credential]
    public PSCredential? Credential { get; set; }

    /// <summary>Trusts the SQL Server TLS certificate without validating the certificate chain.</summary>
    [Parameter(Mandatory = false, ParameterSetName = "Query")]
    [Parameter(Mandatory = false, ParameterSetName = "QueryReader")]
    [Parameter(Mandatory = false, ParameterSetName = "StoredProcedure")]
    public SwitchParameter TrustServerCertificate { get; set; }

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
        var returnDataReader = AsDataReader.IsPresent;
        if (returnDataReader &&
            (Stream.IsPresent || MyInvocation.BoundParameters.ContainsKey(nameof(ReturnType)))) {
            throw new ArgumentException(
                "AsDataReader cannot be combined with Stream or ReturnType when enabled.",
                nameof(AsDataReader));
        }
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
                        } else if (ReturnType == ReturnType.DataRow) {
                            WriteObject(result, true);
                        } else {
                            WriteObject(result);
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
                        } else if (ReturnType == ReturnType.DataRow) {
                            WriteObject(result, true);
                        } else {
                            WriteObject(result);
                        }
                    }
                    return;
                }
            }

            var (resolvedUsername, resolvedPassword, integratedSecurity) = PowerShellHelpers.ResolveSqlServerCredential(Username, Password, Credential);
            var connectionString = DBAClientX.SqlServer.BuildConnectionString(
                Server,
                Database,
                integratedSecurity,
                resolvedUsername,
                resolvedPassword,
                trustServerCertificate: TrustServerCertificate.IsPresent);
            if (returnDataReader)
            {
                await WriteDataReaderAsync(connectionString, parameters).ConfigureAwait(false);
                return;
            }

            if (!PowerShellHelpers.TryValidateConnection(this, "sqlserver", connectionString, ErrorAction))
            {
                return;
            }

            if (ReturnType == ReturnType.PSObject && string.IsNullOrEmpty(StoredProcedure))
            {
                using var psObjectSqlServer = CreateSqlServer();
                string[] columnNames = Array.Empty<string>();
                object[] values = Array.Empty<object>();
                void Initialize(IDataRecord record)
                {
                    columnNames = PSObjectConverter.GetUniqueColumnNames(record);
                    values = new object[columnNames.Length];
                }

                PSObject Map(IDataRecord record) =>
                    PSObjectConverter.DataRecordToPSObject(record, columnNames, values);

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
                if (Stream.IsPresent)
                {
                    await WritePSObjectsAsync(psObjectSqlServer.QueryStreamAsync(
                        connectionString,
                        Query,
                        Map,
                        parameters,
                        cancellationToken: CancelToken,
                        initialize: Initialize)).ConfigureAwait(false);
                }
                else
                {
                    var rows = await psObjectSqlServer.QueryAsListAsync(
                        connectionString,
                        Query,
                        Map,
                        parameters,
                        cancellationToken: CancelToken,
                        initialize: Initialize).ConfigureAwait(false);
                    foreach (var row in rows)
                    {
                        WriteObject(row);
                    }
                }
#else
                var rows = await psObjectSqlServer.QueryAsListAsync(
                    connectionString,
                    Query,
                    Map,
                    parameters,
                    cancellationToken: CancelToken,
                    initialize: Initialize).ConfigureAwait(false);
                foreach (var row in rows)
                {
                    WriteObject(row);
                }
#endif
                return;
            }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent)
            {
                if (!string.IsNullOrEmpty(StoredProcedure))
                {
                    var dbParameters = PowerShellHelpers.ToDbParameters(parameters, static (name, value) => (DbParameter)new SqlParameter(name, value ?? DBNull.Value));
                    using var sqlServer = CreateSqlServer();
                    await WriteRowsAsync(sqlServer.ExecuteStoredProcedureStreamAsync(connectionString, StoredProcedure, dbParameters, cancellationToken: CancelToken)).ConfigureAwait(false);
                    return;
                }

                using var streamSqlServer = CreateSqlServer();
                await WriteRowsAsync(streamSqlServer.QueryStreamAsync(connectionString, Query, parameters, cancellationToken: CancelToken)).ConfigureAwait(false);
                return;
            }
#endif

            if (!string.IsNullOrEmpty(StoredProcedure)) {
                var dbParameters = PowerShellHelpers.ToDbParameters(parameters, static (name, value) => (DbParameter)new SqlParameter(name, value ?? DBNull.Value));
                using var sqlServer = CreateSqlServer();
                result = sqlServer.ExecuteStoredProcedure(connectionString, StoredProcedure, dbParameters);
            } else {
                using var sqlServer = CreateSqlServer();
                result = sqlServer.Query(connectionString, Query, parameters);
            }
            if (result != null) {
                if (ReturnType == ReturnType.PSObject) {
                    //var resultConverted = result as DataTable;
                    foreach (DataRow row in ((DataTable)result).Rows) {
                        WriteObject(PSObjectConverter.DataRowToPSObject(row));
                    }
                } else if (ReturnType == ReturnType.DataRow) {
                    WriteObject(result, true);
                } else {
                    WriteObject(result);
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
        PowerShellHelpers.ApplyQueryTimeout(sqlServer, QueryTimeout, MyInvocation.BoundParameters.ContainsKey(nameof(QueryTimeout)));
        return sqlServer;
    }

    private async Task WriteDataReaderAsync(string connectionString, IDictionary<string, object?>? parameters)
    {
        using var sqlServer = CreateSqlServer();
        DbaDataReader? reader = null;
        try
        {
            reader = await sqlServer.QueryReaderAsync(
                connectionString,
                Query,
                parameters,
                cancellationToken: CancelToken).ConfigureAwait(false);
            WriteObjectAndWait(reader, enumerateCollection: false);
            reader = null;
        }
        finally
        {
            if (reader != null)
            {
                await reader.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    private async Task WritePSObjectsAsync(IAsyncEnumerable<PSObject> rows)
    {
        await foreach (var row in rows.ConfigureAwait(false))
        {
            WriteObject(row);
        }
    }
#endif

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
