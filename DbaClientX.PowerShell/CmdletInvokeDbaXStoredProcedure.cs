namespace DBAClientX.PowerShell;

/// <summary>Invokes a stored procedure through a DbaClientX provider.</summary>
/// <example>
/// <summary>Execute a SQL Server stored procedure.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXStoredProcedure -Provider SqlServer -ConnectionString $connectionString -Procedure dbo.GetUsers -ReturnType PSObject</code>
/// <para>Executes the stored procedure and converts rows to PowerShell objects.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXStoredProcedure", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXStoredProcedure : AsyncPSCmdlet
{
    /// <summary>Provider used to execute the stored procedure.</summary>
    [Parameter(Mandatory = true)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Provider connection string.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Stored procedure name.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string Procedure { get; set; } = string.Empty;

    /// <summary>Optional procedure parameters.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? Parameters { get; set; }

    /// <summary>Executes through an active provider transaction when supported.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter UseTransaction { get; set; }

    /// <summary>Streams result rows instead of buffering the result.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter Stream { get; set; }

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
        PowerShellHelpers.RejectFullConnectionTransactionSwitch(UseTransaction, MyInvocation.MyCommand.Name);

        if (Provider == DbaXProvider.SQLite)
        {
            throw new PSArgumentException("SQLite does not expose stored procedure execution.", nameof(Provider));
        }

        if (!ShouldProcess(Procedure, $"Execute {Provider} stored procedure"))
        {
            return;
        }

        var parameters = PowerShellHelpers.ToDictionaryOrNull(Parameters);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
        if (Stream.IsPresent)
        {
            if (Provider != DbaXProvider.SqlServer)
            {
                throw new NotSupportedException("Stored procedure streaming with a full connection string is currently exposed by DbaClientX for SQL Server only.");
            }

            switch (Provider)
            {
                case DbaXProvider.SqlServer:
                    using (var client = new DBAClientX.SqlServer { ReturnType = ReturnType, CommandTimeout = QueryTimeout })
                    {
                        if (RequiresBufferedAggregate())
                        {
                            DbaXResultWriter.WriteResult(await client.ExecuteStoredProcedureAsync(ConnectionString, Procedure, parameters, UseTransaction.IsPresent, CancelToken).ConfigureAwait(false), ReturnType, WriteObject);
                        }
                        else
                        {
                            await DbaXResultWriter.WriteRowsAsync(client.ExecuteStoredProcedureStreamAsync(ConnectionString, Procedure, parameters, UseTransaction.IsPresent, CancelToken), ReturnType, WriteObject).ConfigureAwait(false);
                        }
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

        object? result = Provider switch
        {
            DbaXProvider.SqlServer => await ExecuteSqlServerAsync(parameters).ConfigureAwait(false),
            DbaXProvider.PostgreSql => await ExecutePostgreSqlAsync(parameters).ConfigureAwait(false),
            DbaXProvider.MySql => await ExecuteMySqlAsync(parameters).ConfigureAwait(false),
            DbaXProvider.Oracle => await ExecuteOracleAsync(parameters).ConfigureAwait(false),
            _ => throw new PSArgumentException($"Provider '{Provider}' is not supported.", nameof(Provider))
        };
        DbaXResultWriter.WriteResult(result, ReturnType, WriteObject);
    }

    private async Task<object?> ExecuteSqlServerAsync(IDictionary<string, object?>? parameters)
    {
        using var client = new DBAClientX.SqlServer { ReturnType = ReturnType, CommandTimeout = QueryTimeout };
        return await client.ExecuteStoredProcedureAsync(ConnectionString, Procedure, parameters, UseTransaction.IsPresent, CancelToken).ConfigureAwait(false);
    }

    private async Task<object?> ExecutePostgreSqlAsync(IDictionary<string, object?>? parameters)
    {
        using var client = new DBAClientX.PostgreSql { ReturnType = ReturnType, CommandTimeout = QueryTimeout };
        return await client.ExecuteStoredProcedureAsync(ConnectionString, Procedure, parameters, UseTransaction.IsPresent, CancelToken).ConfigureAwait(false);
    }

    private async Task<object?> ExecuteMySqlAsync(IDictionary<string, object?>? parameters)
    {
        using var client = new DBAClientX.MySql { ReturnType = ReturnType, CommandTimeout = QueryTimeout };
        return await client.ExecuteStoredProcedureAsync(ConnectionString, Procedure, parameters, UseTransaction.IsPresent, CancelToken).ConfigureAwait(false);
    }

    private async Task<object?> ExecuteOracleAsync(IDictionary<string, object?>? parameters)
    {
        using var client = new DBAClientX.Oracle { ReturnType = ReturnType, CommandTimeout = QueryTimeout };
        return await client.ExecuteStoredProcedureAsync(ConnectionString, Procedure, parameters, UseTransaction.IsPresent, CancelToken).ConfigureAwait(false);
    }

    private bool RequiresBufferedAggregate()
        => ReturnType == ReturnType.DataTable || ReturnType == ReturnType.DataSet;
}
