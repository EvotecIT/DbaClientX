namespace DBAClientX.PowerShell;

/// <summary>Invokes commands against an Oracle database.</summary>
/// <para>Connects to an Oracle server using provided credentials and executes a SQL query.</para>
/// <para>Results can be returned in different formats based on the <see cref="ReturnType"/>.</para>
/// <list type="alertSet">
/// <item>
/// <term>Note</term>
/// <description>Network operations may incur latency; consider using <c>-Stream</c> for large result sets.</description>
/// </item>
/// </list>
/// <example>
/// <summary>Query Oracle with credentials.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXOracle -Server 'oraclesrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT * FROM Users'</code>
/// <para>Returns each row as a <see cref="DataRow"/>.</para>
/// </example>
/// <example>
/// <summary>Stream results as they arrive.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXOracle -Server 'oraclesrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT * FROM Logs' -Stream</code>
/// <para>Streams rows without buffering the entire result.</para>
/// </example>
/// <seealso href="https://learn.microsoft.com/dotnet/standard/data/sqlite/?tabs=netcore-cli">Oracle provider documentation</seealso>
/// <seealso href="https://github.com/EvotecIT/DbaClientX">Project documentation</seealso>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXOracle", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXOracle : AsyncPSCmdlet {
    internal static Func<DBAClientX.Oracle> OracleFactory { get; set; } = () => new DBAClientX.Oracle();
    internal static ScriptBlock? QueryOverride { get; set; }
    internal static ScriptBlock? QueryStreamOverride { get; set; }

    /// <summary>Specifies the Oracle server to connect to.</summary>
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
    [Parameter]
    public string Username { get; set; } = string.Empty;

    /// <summary>Password for authentication.</summary>
    [Parameter]
    public string Password { get; set; } = string.Empty;

    /// <summary>Credential for authentication.</summary>
    [Parameter]
    [Credential]
    public PSCredential? Credential { get; set; }

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
        if (!ShouldProcess($"{Server}/{Database}", "Execute Oracle query")) {
            return;
        }
        var (resolvedUsername, resolvedPassword) = PowerShellHelpers.ResolveExplicitCredential(Username, Password, Credential, "Oracle");
        var connectionString = DBAClientX.Oracle.BuildConnectionString(Server, Database, resolvedUsername, resolvedPassword);
        if (!PowerShellHelpers.TryValidateConnection(this, "oracle", connectionString, ErrorAction))
        {
            return;
        }
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
            if (Stream.IsPresent) {
                if (QueryStreamOverride is not null) {
                    WriteRows(PowerShellHelpers.InvokeDataRowOverride(QueryStreamOverride, this, parameters));
                    return;
                }

                using var oracle = CreateOracle();
                await WriteRowsAsync(oracle.QueryStreamAsync(Server, Database, resolvedUsername, resolvedPassword, Query, parameters, cancellationToken: CancelToken)).ConfigureAwait(false);
                return;
            }
#else
            if (Stream.IsPresent) {
                throw new NotSupportedException("Streaming is not supported on this platform.");
            }
#endif
            object? result;
            if (QueryOverride is not null) {
                result = await PowerShellHelpers.InvokeOverrideAsync<object?>(QueryOverride, this, parameters, resolvedUsername, resolvedPassword).ConfigureAwait(false);
            } else {
                using var oracle = CreateOracle();
                result = await oracle.QueryAsync(Server, Database, resolvedUsername, resolvedPassword, Query, parameters, cancellationToken: CancelToken).ConfigureAwait(false);
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
            WriteWarning($"Invoke-DbaXOracle - Error querying Oracle: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }

    private DBAClientX.Oracle CreateOracle() {
        var oracle = OracleFactory();
        oracle.ReturnType = ReturnType;
        oracle.CommandTimeout = QueryTimeout;
        return oracle;
    }

    private void WriteRows(IEnumerable<DataRow> rows) {
        switch (ReturnType) {
            case ReturnType.DataRow:
                foreach (var row in rows) {
                    WriteObject(row);
                }
                break;
            case ReturnType.DataTable:
                DataTable? table = null;
                foreach (var row in rows) {
                    table ??= row.Table.Clone();
                    table.ImportRow(row);
                }
                if (table != null) {
                    WriteObject(table);
                }
                break;
            case ReturnType.DataSet:
                DataTable? dataTable = null;
                foreach (var row in rows) {
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
                foreach (var row in rows) {
                    WriteObject(PSObjectConverter.DataRowToPSObject(row));
                }
                break;
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    private async Task WriteRowsAsync(IAsyncEnumerable<DataRow> rows) {
        switch (ReturnType) {
            case ReturnType.DataRow:
                await foreach (var row in rows.ConfigureAwait(false)) {
                    WriteObject(row);
                }
                break;
            case ReturnType.DataTable:
                DataTable? table = null;
                await foreach (var row in rows.ConfigureAwait(false)) {
                    table ??= row.Table.Clone();
                    table.ImportRow(row);
                }
                if (table != null) {
                    WriteObject(table);
                }
                break;
            case ReturnType.DataSet:
                DataTable? dataTable = null;
                await foreach (var row in rows.ConfigureAwait(false)) {
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
                await foreach (var row in rows.ConfigureAwait(false)) {
                    WriteObject(PSObjectConverter.DataRowToPSObject(row));
                }
                break;
        }
    }
#endif
}
