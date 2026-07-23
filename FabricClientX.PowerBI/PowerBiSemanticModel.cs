using System.Text.Json.Serialization;

namespace FabricClientX.PowerBI;

/// <summary>Represents a Power BI semantic model returned by the datasets API.</summary>
public sealed class PowerBiSemanticModel
{
    /// <summary>Gets or sets the semantic-model identifier.</summary>
    [JsonPropertyName("id")]
    public Guid Id { get; set; }

    /// <summary>Gets or sets the semantic-model name.</summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = string.Empty;

    /// <summary>Gets or sets the semantic-model web URL.</summary>
    [JsonPropertyName("webUrl")]
    public Uri? WebUrl { get; set; }

    /// <summary>Gets or sets whether configured refresh is available.</summary>
    [JsonPropertyName("isRefreshable")]
    public bool? IsRefreshable { get; set; }

    /// <summary>Gets or sets whether an effective identity is required.</summary>
    [JsonPropertyName("isEffectiveIdentityRequired")]
    public bool? IsEffectiveIdentityRequired { get; set; }
}
