using System.Text.Json.Serialization;

namespace FabricClientX;

/// <summary>Represents a Microsoft Fabric workspace visible to the caller.</summary>
public sealed class FabricWorkspace
{
    /// <summary>Gets or sets the workspace identifier.</summary>
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    /// <summary>Gets or sets the workspace display name.</summary>
    [JsonPropertyName("displayName")]
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>Gets or sets the optional workspace description.</summary>
    [JsonPropertyName("description")]
    public string? Description { get; set; }

    /// <summary>Gets or sets the service-defined workspace type.</summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty;

    /// <summary>Gets or sets the assigned capacity identifier.</summary>
    [JsonPropertyName("capacityId")]
    public Guid? CapacityId { get; set; }

    /// <summary>Gets or sets the workspace-specific API endpoint when requested.</summary>
    [JsonPropertyName("apiEndpoint")]
    public Uri? ApiEndpoint { get; set; }
}
