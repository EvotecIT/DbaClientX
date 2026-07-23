using System.Text.Json.Serialization;

namespace FabricClientX;

/// <summary>Represents an item in a Microsoft Fabric workspace.</summary>
public sealed class FabricItem
{
    /// <summary>Gets or sets the item identifier.</summary>
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    /// <summary>Gets or sets the item display name.</summary>
    [JsonPropertyName("displayName")]
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>Gets or sets the optional item description.</summary>
    [JsonPropertyName("description")]
    public string? Description { get; set; }

    /// <summary>Gets or sets the service-defined item type.</summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = string.Empty;

    /// <summary>Gets or sets the owning workspace identifier.</summary>
    [JsonPropertyName("workspaceId")]
    public Guid WorkspaceId { get; set; }
}
