using System.Text.Json.Serialization;

namespace FabricClientX.PowerBI;

/// <summary>Defines a Power BI semantic-model refresh request.</summary>
public sealed class PowerBiRefreshRequest
{
    /// <summary>
    /// Gets or sets the mail notification mode. Omit it for service-principal enhanced refreshes.
    /// </summary>
    [JsonPropertyName("notifyOption")]
    public string? NotifyOption { get; set; } = "NoNotification";

    /// <summary>Gets or sets the refresh processing type.</summary>
    [JsonPropertyName("type")]
    public string? Type { get; set; }

    /// <summary>Gets or sets the transaction commit behavior.</summary>
    [JsonPropertyName("commitMode")]
    public string? CommitMode { get; set; }

    /// <summary>Gets or sets the maximum number of parallel processing threads.</summary>
    [JsonPropertyName("maxParallelism")]
    public int? MaxParallelism { get; set; }

    /// <summary>Gets or sets the service-side retry count.</summary>
    [JsonPropertyName("retryCount")]
    public int? RetryCount { get; set; }

    /// <summary>Gets or sets the per-attempt timeout in HH:mm:ss form.</summary>
    [JsonPropertyName("timeout")]
    public string? Timeout { get; set; }

    /// <summary>Gets or sets whether an incremental refresh policy is applied.</summary>
    [JsonPropertyName("applyRefreshPolicy")]
    public bool? ApplyRefreshPolicy { get; set; }

    /// <summary>Gets or sets optional table or partition targets.</summary>
    [JsonPropertyName("objects")]
    public IReadOnlyList<PowerBiRefreshObject>? Objects { get; set; }
}

/// <summary>Identifies a table or partition to refresh.</summary>
public sealed class PowerBiRefreshObject
{
    /// <summary>Gets or sets the table name.</summary>
    [JsonPropertyName("table")]
    public string Table { get; set; } = string.Empty;

    /// <summary>Gets or sets the optional partition name.</summary>
    [JsonPropertyName("partition")]
    public string? Partition { get; set; }
}
