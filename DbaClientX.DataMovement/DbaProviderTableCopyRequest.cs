namespace DBAClientX.DataMovement;

/// <summary>
/// Describes a provider-backed table-copy operation without requiring callers to create adapters directly.
/// </summary>
public sealed class DbaProviderTableCopyRequest
{
    /// <summary>Source provider connection and read behavior.</summary>
    public DbaProviderTableCopyAdapterOptions Source { get; set; } = new();

    /// <summary>Destination provider connection and write behavior.</summary>
    public DbaProviderTableCopyAdapterOptions Destination { get; set; } = new();

    /// <summary>One or more table definitions to copy in declared order.</summary>
    public IReadOnlyList<DbaTableCopyDefinition> Definitions { get; set; } = Array.Empty<DbaTableCopyDefinition>();

    /// <summary>Copy execution options such as page size, batch size, clearing, verification, and progress.</summary>
    public DbaTableCopyOptions? Options { get; set; }
}
