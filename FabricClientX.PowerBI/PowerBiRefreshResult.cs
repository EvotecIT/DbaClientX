using System.Text.Json.Serialization;

namespace FabricClientX.PowerBI;

/// <summary>Contains the accepted Power BI refresh identity and correlation metadata.</summary>
public sealed class PowerBiRefreshStartResult
{
    internal PowerBiRefreshStartResult(
        Guid workspaceId,
        Guid semanticModelId,
        Guid refreshId,
        Uri location,
        string operationId)
    {
        WorkspaceId = workspaceId;
        SemanticModelId = semanticModelId;
        RefreshId = refreshId;
        Location = location;
        OperationId = operationId;
    }

    /// <summary>Gets the workspace identifier.</summary>
    public Guid WorkspaceId { get; }

    /// <summary>Gets the semantic-model identifier.</summary>
    public Guid SemanticModelId { get; }

    /// <summary>Gets the Power BI refresh identifier.</summary>
    public Guid RefreshId { get; }

    /// <summary>Gets the refresh status endpoint.</summary>
    public Uri Location { get; }

    /// <summary>Gets the cross-library W3C operation identifier.</summary>
    public string OperationId { get; }
}

/// <summary>Represents safe Power BI refresh execution details.</summary>
public sealed class PowerBiRefreshDetail
{
    /// <summary>Gets or sets the general refresh status.</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    /// <summary>Gets or sets the detailed refresh status.</summary>
    [JsonPropertyName("extendedStatus")]
    public string? ExtendedStatus { get; set; }

    /// <summary>Gets or sets the start timestamp.</summary>
    [JsonPropertyName("startTime")]
    public DateTimeOffset? StartTime { get; set; }

    /// <summary>Gets or sets the completion timestamp.</summary>
    [JsonPropertyName("endTime")]
    public DateTimeOffset? EndTime { get; set; }

    /// <summary>Gets or sets the number of service attempts.</summary>
    [JsonPropertyName("numberOfAttempts")]
    public int? NumberOfAttempts { get; set; }

    /// <summary>Gets or sets normalized engine messages.</summary>
    [JsonPropertyName("messages")]
    public IReadOnlyList<PowerBiRefreshMessage>? Messages { get; set; }
}

/// <summary>Represents a normalized Power BI refresh engine message.</summary>
public sealed class PowerBiRefreshMessage
{
    /// <summary>Gets or sets the message type.</summary>
    [JsonPropertyName("type")]
    public string? Type { get; set; }

}

/// <summary>Contains a settled Power BI refresh and correlation metadata.</summary>
public sealed class PowerBiRefreshSettlement
{
    internal PowerBiRefreshSettlement(
        PowerBiRefreshStartResult start,
        PowerBiRefreshDetail detail)
    {
        Start = start;
        Detail = detail;
    }

    /// <summary>Gets the accepted refresh identity.</summary>
    public PowerBiRefreshStartResult Start { get; }

    /// <summary>Gets the terminal refresh details.</summary>
    public PowerBiRefreshDetail Detail { get; }

    /// <summary>Gets whether the refresh completed successfully.</summary>
    public bool Succeeded =>
        string.Equals(Detail.Status, "Completed", StringComparison.OrdinalIgnoreCase);

    /// <summary>Gets the stable cross-library W3C operation identifier.</summary>
    public string OperationId => Start.OperationId;
}
