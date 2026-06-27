namespace DBAClientX.DataMovement;

/// <summary>
/// Controls source-side shaping before rows are copied to the destination.
/// </summary>
public sealed record DbaTableCopySourceOptions(
    IReadOnlyList<string>? DeduplicateByColumns = null,
    IReadOnlyList<string>? DeduplicateOrderByColumns = null,
    bool DeduplicateCaseInsensitive = false)
{
    /// <summary>Gets whether source rows should be reduced to one row per key before copying.</summary>
    public bool HasDeduplication => DeduplicateByColumns is { Count: > 0 };
}
