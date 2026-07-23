using FabricClientX;
using FabricClientX.OfficeIMO;
using FabricClientX.PowerBI;
using OfficeIMO.CSV;

namespace DBAClientX.PowerShell;

/// <summary>Plans and executes an OfficeIMO CSV to Fabric Warehouse and optional Power BI workflow.</summary>
/// <example>
/// <summary>Stream a CSV into Warehouse and settle a semantic-model refresh.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXFabricCsvWorkflow -CsvPath .\sales.csv -SourceName Sales -WarehouseConnectionString $warehouse -WarehouseConnectionOptions $warehouseOptions -DestinationTable dbo.Sales -Refresh -PowerBiTokenProvider $powerBiProvider -WorkspaceId $workspaceId -SemanticModelId $modelId -Wait</code>
/// <para>Creates a redacted plan before performing the confirmed Warehouse write and refresh.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXFabricCsvWorkflow", SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.High)]
[OutputType(typeof(CsvFabricWorkflowResult))]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXFabricCsvWorkflow : AsyncPSCmdlet
{
    /// <summary>Path to an OfficeIMO-compatible CSV file.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    [ValidateNotNullOrEmpty]
    public string CsvPath { get; set; } = string.Empty;

    /// <summary>Logical source name used in the redacted plan.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string SourceName { get; set; } = string.Empty;

    /// <summary>Fabric Warehouse connection string without embedded credentials.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string WarehouseConnectionString { get; set; } = string.Empty;

    /// <summary>Caller-owned Fabric Warehouse connection and authentication options.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public SqlServerConnectionOptions WarehouseConnectionOptions { get; set; } = null!;

    /// <summary>Warehouse destination table.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationTable { get; set; } = string.Empty;

    /// <summary>Optional OfficeIMO CSV parsing options.</summary>
    [Parameter]
    [ValidateNotNull]
    public CsvLoadOptions? CsvLoadOptions { get; set; }

    /// <summary>Optional OfficeIMO CSV data-reader projection options.</summary>
    [Parameter]
    [ValidateNotNull]
    public CsvDataReaderOptions? CsvReaderOptions { get; set; }

    /// <summary>Optional SQL bulk-copy batch size.</summary>
    [Parameter]
    [ValidateRange(1, int.MaxValue)]
    public int? BatchSize { get; set; }

    /// <summary>Optional SQL bulk-copy timeout in seconds.</summary>
    [Parameter]
    [ValidateRange(1, int.MaxValue)]
    public int? BulkCopyTimeout { get; set; }

    /// <summary>Requests a Power BI semantic-model refresh after successful ingestion.</summary>
    [Parameter]
    public SwitchParameter Refresh { get; set; }

    /// <summary>Caller-owned token provider configured for the Power BI API scope.</summary>
    [Parameter]
    [ValidateNotNull]
    public IFabricTokenProvider? PowerBiTokenProvider { get; set; }

    /// <summary>Power BI workspace identifier used when Refresh is selected.</summary>
    [Parameter]
    public Guid? WorkspaceId { get; set; }

    /// <summary>Power BI semantic-model identifier used when Refresh is selected.</summary>
    [Parameter]
    public Guid? SemanticModelId { get; set; }

    /// <summary>Waits for the requested refresh to settle.</summary>
    [Parameter]
    public SwitchParameter Wait { get; set; }

    /// <summary>Maximum refresh settlement wait in minutes.</summary>
    [Parameter]
    [ValidateRange(1, 1440)]
    public int TimeoutMinutes { get; set; } = 60;

    /// <summary>Optional non-zero W3C trace identifier used across all workflow stages.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string? OperationId { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        if (Refresh.IsPresent &&
            (PowerBiTokenProvider == null ||
             !WorkspaceId.HasValue ||
             !SemanticModelId.HasValue))
        {
            throw new PSArgumentException(
                "PowerBiTokenProvider, WorkspaceId, and SemanticModelId are required when Refresh is selected.");
        }

        var request = new CsvFabricWorkflowRequest(
            GetUnresolvedProviderPathFromPSPath(CsvPath),
            SourceName,
            WarehouseConnectionString,
            DestinationTable)
        {
            BatchSize = BatchSize,
            BulkCopyTimeout = BulkCopyTimeout,
            RefreshAfterLoad = Refresh.IsPresent,
            WorkspaceId = WorkspaceId,
            SemanticModelId = SemanticModelId,
            WaitForRefresh = Wait.IsPresent,
            RefreshTimeout = TimeSpan.FromMinutes(TimeoutMinutes),
            OperationId = OperationId
        };
        if (CsvLoadOptions != null)
        {
            request.CsvLoadOptions = CsvLoadOptions;
        }

        if (CsvReaderOptions != null)
        {
            request.CsvReaderOptions = CsvReaderOptions;
        }

        var workflow = new CsvFabricWorkflow();
        var plan = workflow.CreatePlan(request);
        WriteVerbose(
            $"Fabric CSV plan {plan.DefinitionFingerprint} uses operation {plan.OperationId}.");
        var action = Refresh.IsPresent
            ? "Stream the CSV into Fabric Warehouse and request a Power BI refresh"
            : "Stream the CSV into Fabric Warehouse";
        if (!ShouldProcess(DestinationTable, action))
        {
            return;
        }

        using var warehouseClient = new SqlServer
        {
            ConnectionOptions = WarehouseConnectionOptions
        };
        PowerBiClient? powerBiClient = PowerBiTokenProvider == null
            ? null
            : FabricPowerShellClients.CreatePowerBiClient(PowerBiTokenProvider);
        var result = await workflow.ExecuteAsync(
            plan,
            warehouseClient,
            powerBiClient,
            CancelToken).ConfigureAwait(false);
        WriteObject(result);
    }
}
