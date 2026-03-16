namespace DBAClientX.PowerShell;

/// <summary>Executes a scalar SQL query against MySQL.</summary>
/// <para>Runs a SQL statement and returns a single value. Supports asynchronous execution and type conversion via <see cref="ReturnType"/>.</para>
/// <example>
/// <summary>Get a single value.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXMySqlScalar -Server 'mysqlsrv' -Database 'app' -Username 'user' -Password 'p@ss' -Query 'SELECT COUNT(*) FROM Users'</code>
/// <para>Returns the number of users.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXMySqlScalar", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXMySqlScalar : AsyncPSCmdlet {
    internal static Func<DBAClientX.MySql> MySqlFactory { get; set; } = () => new DBAClientX.MySql();
    internal static ScriptBlock? ScalarOverride { get; set; }

    /// <summary>Specifies the MySQL server.</summary>
    [Parameter(Mandatory = true)]
    [Alias("DBServer", "SqlInstance", "Instance")]
    [ValidateNotNullOrEmpty]
    public string Server { get; set; } = string.Empty;

    /// <summary>Defines the target database.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Database { get; set; } = string.Empty;

    /// <summary>The SQL command to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Sets the command timeout in seconds.</summary>
    [Parameter]
    public int QueryTimeout { get; set; }

    /// <summary>Selects the format of the returned value.</summary>
    [Parameter]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    /// <summary>Provides parameters for the SQL command.</summary>
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
        using var mySql = MySqlFactory();
        mySql.CommandTimeout = QueryTimeout;
        if (!ShouldProcess($"{Server}/{Database}", "Execute MySQL scalar query")) {
            return;
        }
        var (resolvedUsername, resolvedPassword) = PowerShellHelpers.ResolveExplicitCredential(Username, Password, Credential, "MySQL");
        var connectionString = DBAClientX.MySql.BuildConnectionString(Server, Database, resolvedUsername, resolvedPassword);
        if (!PowerShellHelpers.TryValidateConnection(this, "mysql", connectionString, ErrorAction))
        {
            return;
        }
        try {
            var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
            object? result;
            if (ScalarOverride is not null)
            {
                result = await PowerShellHelpers.InvokeOverrideAsync<object?>(ScalarOverride, this, parameters, resolvedUsername, resolvedPassword).ConfigureAwait(false);
            }
            else
            {
                result = await mySql.ExecuteScalarAsync(Server, Database, resolvedUsername, resolvedPassword, Query, parameters, cancellationToken: CancelToken).ConfigureAwait(false);
            }
            switch (ReturnType) {
                case ReturnType.DataTable:
                    DataTable table = new DataTable();
                    table.Columns.Add("Value", result?.GetType() ?? typeof(object));
                    var tableRow = table.NewRow();
                    tableRow[0] = result;
                    table.Rows.Add(tableRow);
                    WriteObject(table);
                    break;
                case ReturnType.DataSet:
                    DataTable dataTable = new DataTable();
                    dataTable.Columns.Add("Value", result?.GetType() ?? typeof(object));
                    var dataRow = dataTable.NewRow();
                    dataRow[0] = result;
                    dataTable.Rows.Add(dataRow);
                    DataSet set = new DataSet();
                    set.Tables.Add(dataTable);
                    WriteObject(set);
                    break;
                case ReturnType.PSObject:
                    var psObj = new PSObject();
                    psObj.Members.Add(new PSNoteProperty("Value", result));
                    WriteObject(psObj);
                    break;
                default:
                    DataTable dt = new DataTable();
                    dt.Columns.Add("Value", result?.GetType() ?? typeof(object));
                    var row = dt.NewRow();
                    row[0] = result;
                    dt.Rows.Add(row);
                    WriteObject(row);
                    break;
            }
        } catch (Exception ex) {
            WriteWarning($"Invoke-DbaXMySqlScalar - Error executing MySql: {ex.Message}");
            if (ErrorAction == ActionPreference.Stop) {
                throw;
            }
        }
    }
}
