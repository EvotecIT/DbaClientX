namespace DBAClientX.PowerShell;

/// <summary>Streams query rows through a DbaClientX provider.</summary>
/// <example>
/// <summary>Stream SQL Server rows.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXQueryStream -Provider SqlServer -ConnectionString $connectionString -Query 'SELECT name FROM sys.databases' -ReturnType PSObject</code>
/// <para>Streams query rows without buffering the full result set.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXQueryStream", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXQueryStream : AsyncPSCmdlet
{
    /// <summary>Provider used to stream query rows.</summary>
    [Parameter(Mandatory = true)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Provider connection string, or SQLite database path.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Query text to execute.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Query { get; set; } = string.Empty;

    /// <summary>Optional query parameters.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? Parameters { get; set; }

    /// <summary>Executes through an active provider transaction when supported.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter UseTransaction { get; set; }

    /// <summary>Controls the returned object format.</summary>
    [Parameter(Mandatory = false)]
    [Alias("As")]
    public ReturnType ReturnType { get; set; } = ReturnType.DataRow;

    /// <summary>Optional command timeout in seconds.</summary>
    [Parameter(Mandatory = false)]
    public int QueryTimeout { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        if (!ShouldProcess(ConnectionString, $"Stream {Provider} query"))
        {
            return;
        }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
        switch (Provider)
        {
            case DbaXProvider.SqlServer:
                using (var client = new DBAClientX.SqlServer { ReturnType = ReturnType, CommandTimeout = QueryTimeout })
                {
                    await DbaXResultWriter.WriteRowsAsync(client.QueryStreamAsync(ConnectionString, Query, parameters, UseTransaction.IsPresent, CancelToken), ReturnType, WriteObject).ConfigureAwait(false);
                }
                break;
            case DbaXProvider.PostgreSql:
                using (var client = new DBAClientX.PostgreSql { ReturnType = ReturnType, CommandTimeout = QueryTimeout })
                {
                    await DbaXResultWriter.WriteRowsAsync(client.QueryStreamAsync(ConnectionString, Query, parameters, UseTransaction.IsPresent, CancelToken), ReturnType, WriteObject).ConfigureAwait(false);
                }
                break;
            case DbaXProvider.MySql:
                using (var client = new DBAClientX.MySql { ReturnType = ReturnType, CommandTimeout = QueryTimeout })
                {
                    await DbaXResultWriter.WriteRowsAsync(client.QueryStreamAsync(ConnectionString, Query, parameters, UseTransaction.IsPresent, CancelToken), ReturnType, WriteObject).ConfigureAwait(false);
                }
                break;
            case DbaXProvider.Oracle:
                using (var client = new DBAClientX.Oracle { ReturnType = ReturnType, CommandTimeout = QueryTimeout })
                {
                    await DbaXResultWriter.WriteRowsAsync(client.QueryStreamAsync(ConnectionString, Query, parameters, UseTransaction.IsPresent, CancelToken), ReturnType, WriteObject).ConfigureAwait(false);
                }
                break;
            case DbaXProvider.SQLite:
                using (var client = new DBAClientX.SQLite { ReturnType = ReturnType, CommandTimeout = QueryTimeout })
                {
                    await DbaXResultWriter.WriteRowsAsync(client.QueryStreamAsync(DbaXProviderHelpers.GetSQLiteDatabase(ConnectionString), Query, parameters, UseTransaction.IsPresent, CancelToken), ReturnType, WriteObject).ConfigureAwait(false);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{Provider}' is not supported.", nameof(Provider));
        }
#else
        await Task.Yield();
        throw new NotSupportedException("Streaming is not supported on this platform.");
#endif
    }
}
