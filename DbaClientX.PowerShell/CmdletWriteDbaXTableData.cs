using System.Data;
using System.Management.Automation;

namespace DBAClientX.PowerShell;

/// <summary>Writes tabular pipeline input to a database table using provider-native bulk insert APIs.</summary>
/// <para>Accepts a DataTable, DataView, IDataReader, DataRow pipeline, or regular PowerShell objects and routes the resulting table to the selected DbaClientX provider.</para>
/// <example>
/// <summary>Write an Excel-imported DataTable to SQL Server.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Import-OfficeExcel .\Data.xlsx -AsDataTable | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable dbo.Import</code>
/// <para>Loads the workbook rows as a DataTable and sends them through the SQL Server bulk-copy provider.</para>
/// </example>
/// <example>
/// <summary>Write object pipeline data to PostgreSQL.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$rows | Write-DbaXTableData -Provider PostgreSql -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret' -DestinationTable public.import_data -BatchSize 5000</code>
/// <para>Converts the objects to a DataTable and writes them with the PostgreSQL COPY-backed provider.</para>
/// </example>
[Cmdlet(VerbsCommunications.Write, "DbaXTableData", SupportsShouldProcess = true)]
[CmdletBinding()]
public sealed class CmdletWriteDbaXTableData : PSCmdlet
{
    internal static ScriptBlock? BulkInsertOverride { get; set; }

    private readonly List<object?> _input = new();
    private ActionPreference _errorAction;

    /// <summary>Database provider that should receive the bulk insert.</summary>
    [Parameter(Mandatory = true)]
    public DbaXBulkProvider Provider { get; set; }

    /// <summary>Provider connection string used for the bulk insert.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Destination table name. Include schema or owner when required by the provider.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Tabular input to write. Accepts DataTable, DataView, IDataReader, DataRow, hashtable, and object pipeline input.</summary>
    [Parameter(ValueFromPipeline = true, Mandatory = true)]
    public object? InputObject { get; set; }

    /// <summary>Optional number of rows per provider bulk batch.</summary>
    [Parameter]
    public int? BatchSize { get; set; }

    /// <summary>Optional provider bulk-copy timeout in seconds. SQLite does not support this option.</summary>
    [Parameter]
    public int? BulkCopyTimeout { get; set; }

    /// <summary>Writes a small result object with provider, destination table, and row count.</summary>
    [Parameter]
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
        try
        {
            ValidateOptions();

            if (!ShouldProcess(DestinationTable, $"Bulk write {_input.Count} input item(s) using {Provider}"))
            {
                return;
            }

            var table = PowerShellDataTableConverter.ToDataTable(_input, DestinationTable);
            var providerAlias = GetProviderAlias(Provider);
            if (!PowerShellHelpers.TryValidateConnection(this, providerAlias, ConnectionString, _errorAction))
            {
                return;
            }

            if (BulkInsertOverride != null)
            {
                BulkInsertOverride.InvokeWithContext(
                    functionsToDefine: null,
                    variablesToDefine: null,
                    args: new object?[] { this, table });
            }
            else
            {
                InvokeProviderBulkInsert(table);
            }

            if (PassThru.IsPresent)
            {
                WriteObject(new PSObject(new
                {
                    Provider,
                    DestinationTable,
                    Rows = table.Rows.Count
                }));
            }
        }
        catch (Exception ex)
        {
            WriteWarning($"Write-DbaXTableData - Error writing table data: {ex.Message}");
            if (_errorAction == ActionPreference.Stop)
            {
                throw;
            }
        }
    }

    private void ValidateOptions()
    {
        if (BatchSize.HasValue && BatchSize.Value <= 0)
        {
            throw new PSArgumentException("BatchSize must be greater than zero.", nameof(BatchSize));
        }

        if (BulkCopyTimeout.HasValue && BulkCopyTimeout.Value <= 0)
        {
            throw new PSArgumentException("BulkCopyTimeout must be greater than zero.", nameof(BulkCopyTimeout));
        }

        if (Provider == DbaXBulkProvider.SQLite && BulkCopyTimeout.HasValue)
        {
            throw new PSArgumentException("SQLite bulk inserts do not support BulkCopyTimeout.", nameof(BulkCopyTimeout));
        }
    }

    private void InvokeProviderBulkInsert(DataTable table)
    {
        switch (Provider)
        {
            case DbaXBulkProvider.SqlServer:
                using (var sqlServer = new DBAClientX.SqlServer())
                {
                    sqlServer.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.PostgreSql:
                using (var postgreSql = new DBAClientX.PostgreSql())
                {
                    postgreSql.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.MySql:
                using (var mySql = new DBAClientX.MySql())
                {
                    mySql.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.Oracle:
                using (var oracle = new DBAClientX.Oracle())
                {
                    oracle.BulkInsert(ConnectionString, table, DestinationTable, batchSize: BatchSize, bulkCopyTimeout: BulkCopyTimeout);
                }
                break;
            case DbaXBulkProvider.SQLite:
                using (var sqlite = new DBAClientX.SQLite())
                {
                    sqlite.BulkInsertWithConnectionString(ConnectionString, table, DestinationTable, batchSize: BatchSize);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{Provider}' is not supported.", nameof(Provider));
        }
    }

    private static string GetProviderAlias(DbaXBulkProvider provider)
        => provider switch
        {
            DbaXBulkProvider.SqlServer => "sqlserver",
            DbaXBulkProvider.PostgreSql => "postgresql",
            DbaXBulkProvider.MySql => "mysql",
            DbaXBulkProvider.Oracle => "oracle",
            DbaXBulkProvider.SQLite => "sqlite",
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.", nameof(provider))
        };
}
