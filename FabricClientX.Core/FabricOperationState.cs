using System.Text.Json.Serialization;

namespace FabricClientX;

/// <summary>Represents a Microsoft Fabric long-running operation state.</summary>
public sealed class FabricOperationState
{
    /// <summary>Gets or sets the service-defined status.</summary>
    [JsonPropertyName("status")]
    public string Status { get; set; } = string.Empty;

    /// <summary>Gets or sets the creation timestamp.</summary>
    [JsonPropertyName("createdTimeUtc")]
    public DateTimeOffset? CreatedTimeUtc { get; set; }

    /// <summary>Gets or sets the last-updated timestamp.</summary>
    [JsonPropertyName("lastUpdatedTimeUtc")]
    public DateTimeOffset? LastUpdatedTimeUtc { get; set; }

    /// <summary>Gets or sets completion progress.</summary>
    [JsonPropertyName("percentComplete")]
    public int? PercentComplete { get; set; }

    /// <summary>Gets or sets normalized failure details.</summary>
    [JsonPropertyName("error")]
    public FabricOperationError? Error { get; set; }
}

/// <summary>Contains safe Microsoft Fabric operation failure details.</summary>
public sealed class FabricOperationError
{
    /// <summary>Gets or sets the service error code.</summary>
    [JsonPropertyName("errorCode")]
    public string? ErrorCode { get; set; }

    /// <summary>Gets or sets the request identifier.</summary>
    [JsonPropertyName("requestId")]
    public string? RequestId { get; set; }
}

/// <summary>Contains a completed Fabric operation state and result.</summary>
public sealed class FabricOperationResult<T>
{
    internal FabricOperationResult(
        FabricOperationState state,
        T? value,
        string operationId,
        Guid serviceOperationId)
    {
        State = state;
        Value = value;
        OperationId = operationId;
        ServiceOperationId = serviceOperationId;
    }

    /// <summary>Gets the terminal operation state.</summary>
    public FabricOperationState State { get; }

    /// <summary>Gets the operation result.</summary>
    public T? Value { get; }

    /// <summary>Gets the stable cross-library W3C operation identifier.</summary>
    public string OperationId { get; }

    /// <summary>Gets the Microsoft Fabric service operation identifier.</summary>
    public Guid ServiceOperationId { get; }
}
