namespace DBAClientX.DataMovement;

/// <summary>
/// Exposes a provider identity to provider-neutral diagnostics without exposing connection details.
/// </summary>
public interface IDbaTableCopyProviderIdentity
{
    /// <summary>Provider implemented by the adapter.</summary>
    DbaTableCopyProvider Provider { get; }
}
