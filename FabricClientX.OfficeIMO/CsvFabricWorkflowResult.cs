using FabricClientX.PowerBI;

namespace FabricClientX.OfficeIMO;

/// <summary>Contains OfficeIMO ingestion and optional Power BI refresh results.</summary>
public sealed class CsvFabricWorkflowResult
{
    internal CsvFabricWorkflowResult(
        CsvFabricWorkflowPlan plan,
        long rowsCopied,
        PowerBiRefreshStartResult? refreshStart,
        PowerBiRefreshSettlement? refreshSettlement)
    {
        Plan = plan;
        RowsCopied = rowsCopied;
        RefreshStart = refreshStart;
        RefreshSettlement = refreshSettlement;
    }

    /// <summary>Gets the executed redacted plan.</summary>
    public CsvFabricWorkflowPlan Plan { get; }

    /// <summary>Gets the Warehouse provider-reported row count.</summary>
    public long RowsCopied { get; }

    /// <summary>Gets the accepted refresh when one was requested.</summary>
    public PowerBiRefreshStartResult? RefreshStart { get; }

    /// <summary>Gets the terminal refresh when settlement was requested.</summary>
    public PowerBiRefreshSettlement? RefreshSettlement { get; }

    /// <summary>Gets the stable operation identifier across parsing, ingestion, and refresh.</summary>
    public string OperationId => Plan.OperationId;
}
