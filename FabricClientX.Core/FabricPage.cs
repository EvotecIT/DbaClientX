using System.Text.Json.Serialization;

namespace FabricClientX;

/// <summary>Represents one page returned by a Microsoft Fabric collection endpoint.</summary>
public sealed class FabricPage<T>
{
    /// <summary>Gets or sets the page values.</summary>
    [JsonPropertyName("value")]
    public IReadOnlyList<T> Value { get; set; } = Array.Empty<T>();

    /// <summary>Gets or sets the opaque continuation token.</summary>
    [JsonPropertyName("continuationToken")]
    public string? ContinuationToken { get; set; }

    /// <summary>Gets or sets the service-provided URI for the next page.</summary>
    [JsonPropertyName("continuationUri")]
    public string? ContinuationUri { get; set; }
}
