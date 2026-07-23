namespace FabricClientX.OfficeIMO;

/// <summary>Contains a redacted, deterministic plan for an OfficeIMO CSV Fabric workflow.</summary>
public sealed class CsvFabricWorkflowPlan
{
    internal CsvFabricWorkflowPlan(
        CsvFabricWorkflowRequest request,
        string operationId,
        string definitionFingerprint)
    {
        Request = request;
        OperationId = operationId;
        DefinitionFingerprint = definitionFingerprint;
        SourceName = request.SourceName.Trim();
        DestinationTable = request.DestinationTable.Trim();
        RefreshAfterLoad = request.RefreshAfterLoad;
        WorkspaceId = request.WorkspaceId;
        SemanticModelId = request.SemanticModelId;
    }

    internal CsvFabricWorkflowRequest Request { get; }

    /// <summary>Gets the logical source name.</summary>
    public string SourceName { get; }

    /// <summary>Gets the destination table.</summary>
    public string DestinationTable { get; }

    /// <summary>Gets whether the workflow includes a Power BI refresh.</summary>
    public bool RefreshAfterLoad { get; }

    /// <summary>Gets the optional Power BI workspace identifier.</summary>
    public Guid? WorkspaceId { get; }

    /// <summary>Gets the optional Power BI semantic-model identifier.</summary>
    public Guid? SemanticModelId { get; }

    /// <summary>Gets the stable W3C operation identifier.</summary>
    public string OperationId { get; }

    /// <summary>Gets a deterministic SHA-256 fingerprint of the safe workflow definition.</summary>
    public string DefinitionFingerprint { get; }

    /// <summary>Gets whether the plan uses the preview Fabric direct bulk-copy API.</summary>
    public bool UsesPreviewDirectBulkCopy => true;
}
