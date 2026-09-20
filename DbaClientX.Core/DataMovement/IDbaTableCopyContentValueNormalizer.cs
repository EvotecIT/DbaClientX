namespace DBAClientX.DataMovement;

/// <summary>
/// Normalizes provider-specific values before table-copy content verification hashes them.
/// </summary>
/// <remarks>
/// Implement this interface on a table-copy source when its provider exposes values that the
/// provider-neutral content hasher cannot represent directly. The returned value must be stable,
/// lossless for the provider value, and composed of values supported by content verification.
/// </remarks>
public interface IDbaTableCopyContentValueNormalizer
{
    /// <summary>Returns a provider-neutral representation of a value for content verification.</summary>
    /// <param name="value">Provider value read from a table-copy page.</param>
    /// <returns>A stable provider-neutral value, or <see langword="null"/> for a database null.</returns>
    object? NormalizeContentValue(object value);
}
