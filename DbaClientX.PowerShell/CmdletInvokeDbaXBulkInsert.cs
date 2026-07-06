using System.Data;

namespace DBAClientX.PowerShell;

/// <summary>Invokes a provider-native DbaClientX bulk insert from tabular PowerShell input.</summary>
/// <example>
/// <summary>Bulk insert object rows into SQL Server.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$rows | Invoke-DbaXBulkInsert -Provider SqlServer -ConnectionString $connectionString -DestinationTable dbo.Import -PassThru</code>
/// <para>Converts pipeline input to a DataTable and writes it using the SQL Server bulk provider.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXBulkInsert", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXBulkInsert : PSCmdlet
{
    private readonly List<object?> _input = new();
    private ActionPreference _errorAction;

    /// <summary>Provider used for the bulk insert.</summary>
    [Parameter(Mandatory = true)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Provider connection string, or SQLite database path.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Destination table name.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Tabular input to write. Accepts DataTable, DataView, IDataReader, DataRow, hashtable, and object pipeline input.</summary>
    [Parameter(Mandatory = true, ValueFromPipeline = true)]
    [AllowNull]
    public object? InputObject { get; set; }

    /// <summary>Optional rows per provider bulk batch.</summary>
    [Parameter(Mandatory = false)]
    public int? BatchSize { get; set; }

    /// <summary>Optional provider bulk-copy timeout in seconds. SQLite does not support this option.</summary>
    [Parameter(Mandatory = false)]
    public int? BulkCopyTimeout { get; set; }

    /// <summary>Executes the bulk insert inside a provider transaction where supported.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter UseTransaction { get; set; }

    /// <summary>Returns a small result object with provider, destination table, and row count.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter PassThru { get; set; }

    /// <inheritdoc />
    protected override void BeginProcessing()
    {
        _errorAction = this.ResolveErrorAction();
    }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        _input.Add(InputObject);
    }

    /// <inheritdoc />
    protected override void EndProcessing()
    {
        if (BatchSize.HasValue && BatchSize.Value <= 0)
        {
            throw new PSArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));
        }

        if (BulkCopyTimeout.HasValue && BulkCopyTimeout.Value <= 0)
        {
            throw new PSArgumentException("BulkCopyTimeout must be greater than zero.", nameof(BulkCopyTimeout));
        }

        if (Provider == DbaXProvider.SQLite && BulkCopyTimeout.HasValue)
        {
            throw new PSArgumentException("SQLite bulk inserts do not support BulkCopyTimeout.", nameof(BulkCopyTimeout));
        }

        PowerShellHelpers.RejectFullConnectionTransactionSwitch(UseTransaction, MyInvocation.MyCommand.Name);

        if (_input.Count == 0)
        {
            WriteResult(0);
            return;
        }

        if (!ShouldProcess(DestinationTable, $"Bulk insert pipeline input using {Provider}"))
        {
            return;
        }

        var table = PowerShellDataTableConverter.ToDataTable(_input, DestinationTable);
        if (table.Rows.Count == 0)
        {
            WriteResult(0);
            return;
        }

        if (Provider == DbaXProvider.MySql &&
            (!PowerShellHelpers.TryValidateConnection(
                this,
                "mysql",
                ConnectionString,
                _errorAction,
                allowedUnsupportedOptions: PowerShellHelpers.MySqlBulkCopyAllowedUnsupportedOptions) ||
             !PowerShellHelpers.TryRequireMySqlBulkCopyLocalInfile(this, ConnectionString, _errorAction)))
        {
            return;
        }

        InvokeProviderBulkInsert(table);
        WriteResult(table.Rows.Count);
    }

    private void WriteResult(int rows)
    {
        if (!PassThru.IsPresent)
        {
            return;
        }

        WriteObject(new PSObject(new
        {
            Provider,
            DestinationTable,
            Rows = rows,
            CompletedAt = DateTimeOffset.UtcNow
        }));
    }

    private void InvokeProviderBulkInsert(DataTable table)
    {
        switch (Provider)
        {
            case DbaXProvider.SqlServer:
                using (var client = new DBAClientX.SqlServer())
                {
                    client.BulkInsert(ConnectionString, table, DestinationTable, UseTransaction.IsPresent, BatchSize, BulkCopyTimeout);
                }
                break;
            case DbaXProvider.PostgreSql:
                using (var client = new DBAClientX.PostgreSql())
                {
                    var (bulkTable, destinationTable) = DbaXProviderHelpers.NormalizeBulkInsertInput(Provider, table, DestinationTable);
                    using var disposableBulkTable = ReferenceEquals(bulkTable, table) ? null : bulkTable;
                    client.BulkInsert(ConnectionString, bulkTable, destinationTable, UseTransaction.IsPresent, BatchSize, BulkCopyTimeout);
                }
                break;
            case DbaXProvider.MySql:
                using (var client = new DBAClientX.MySql())
                {
                    client.BulkInsert(ConnectionString, table, DestinationTable, UseTransaction.IsPresent, BatchSize, BulkCopyTimeout);
                }
                break;
            case DbaXProvider.Oracle:
                using (var client = new DBAClientX.Oracle())
                {
                    client.BulkInsert(ConnectionString, table, DestinationTable, UseTransaction.IsPresent, BatchSize, BulkCopyTimeout);
                }
                break;
            case DbaXProvider.SQLite:
                using (var client = new DBAClientX.SQLite())
                {
                    client.BulkInsertWithConnectionString(
                        DbaXProviderHelpers.GetValidatedSQLiteConnectionString(ConnectionString),
                        table,
                        DestinationTable,
                        UseTransaction.IsPresent,
                        BatchSize);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{Provider}' is not supported.", nameof(Provider));
        }
    }
}
