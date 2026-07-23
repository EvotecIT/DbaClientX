using DBAClientX;
using FabricClientX.PowerBI;
using OfficeIMO.CSV;

namespace FabricClientX.OfficeIMO;

/// <summary>Defines a planned OfficeIMO CSV to Fabric Warehouse and Power BI workflow.</summary>
public sealed class CsvFabricWorkflowRequest
{
    /// <summary>Creates a workflow request.</summary>
    public CsvFabricWorkflowRequest(
        string csvPath,
        string sourceName,
        string warehouseConnectionString,
        string destinationTable)
    {
        CsvPath = csvPath;
        SourceName = sourceName;
        WarehouseConnectionString = warehouseConnectionString;
        DestinationTable = destinationTable;
    }

    /// <summary>Gets the caller-owned CSV path. It is never emitted to diagnostics.</summary>
    public string CsvPath { get; }

    /// <summary>Gets the caller-provided logical source name used in plans and results.</summary>
    public string SourceName { get; }

    /// <summary>Gets the caller-owned Warehouse connection string. It is never emitted to results.</summary>
    public string WarehouseConnectionString { get; }

    /// <summary>Gets the Warehouse destination table.</summary>
    public string DestinationTable { get; }

    /// <summary>Gets or sets OfficeIMO CSV parsing options.</summary>
    public CsvLoadOptions CsvLoadOptions { get; set; } = new();

    /// <summary>Gets or sets OfficeIMO data-reader projection options.</summary>
    public CsvDataReaderOptions CsvReaderOptions { get; set; } = new()
    {
        InferSchema = true
    };

    /// <summary>Gets or sets DbaClientX SQL bulk-copy options.</summary>
    public SqlServerBulkInsertOptions BulkInsertOptions { get; set; } = new()
    {
        AutoCreateTable = true
    };

    /// <summary>Gets or sets the provider bulk-copy batch size.</summary>
    public int? BatchSize { get; set; }

    /// <summary>Gets or sets the provider bulk-copy timeout in seconds.</summary>
    public int? BulkCopyTimeout { get; set; }

    /// <summary>Gets or sets whether a Power BI refresh follows successful ingestion.</summary>
    public bool RefreshAfterLoad { get; set; }

    /// <summary>Gets or sets the Power BI workspace identifier.</summary>
    public Guid? WorkspaceId { get; set; }

    /// <summary>Gets or sets the Power BI semantic-model identifier.</summary>
    public Guid? SemanticModelId { get; set; }

    /// <summary>Gets or sets the typed Power BI refresh request.</summary>
    public PowerBiRefreshRequest RefreshRequest { get; set; } = new();

    /// <summary>Gets or sets whether the workflow waits for refresh settlement.</summary>
    public bool WaitForRefresh { get; set; } = true;

    /// <summary>Gets or sets the maximum refresh settlement wait.</summary>
    public TimeSpan RefreshTimeout { get; set; } = TimeSpan.FromHours(1);

    /// <summary>Gets or sets the refresh polling interval.</summary>
    public TimeSpan RefreshPollInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Gets or sets an optional W3C trace identifier for end-to-end correlation.</summary>
    public string? OperationId { get; set; }
}
