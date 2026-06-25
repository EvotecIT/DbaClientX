namespace DBAClientX.DataMovement;

/// <summary>
/// Runs provider-backed table copies using reusable DbaClientX adapters and the core copy engine.
/// </summary>
public sealed class DbaProviderTableCopyRunner
{
    /// <summary>
    /// Copies tables between provider connections from a single request object.
    /// </summary>
    public Task<DbaTableCopyResult> CopyAsync(DbaProviderTableCopyRequest request, CancellationToken cancellationToken = default)
    {
        if (request == null)
        {
            throw new ArgumentNullException(nameof(request));
        }

        if (request.Source == null)
        {
            throw new ArgumentException("Source options are required.", nameof(request));
        }

        if (request.Destination == null)
        {
            throw new ArgumentException("Destination options are required.", nameof(request));
        }

        if (request.Definitions == null || request.Definitions.Count == 0)
        {
            throw new ArgumentException("At least one table copy definition is required.", nameof(request));
        }

        ValidateSameProviderTableCopy(request);

        var source = new DbaProviderTableCopyAdapter(request.Source);
        var destination = new DbaProviderTableCopyAdapter(request.Destination);
        return new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            request.Definitions,
            request.Options,
            cancellationToken);
    }

    private static void ValidateSameProviderTableCopy(DbaProviderTableCopyRequest request)
    {
        if (request.AllowSameProviderTableCopy ||
            request.Source.Provider != request.Destination.Provider ||
            !DbaProviderTableCopyTargetIdentity.TryCreate(request.Source, out var sourceIdentity) ||
            !DbaProviderTableCopyTargetIdentity.TryCreate(request.Destination, out var destinationIdentity) ||
            !string.Equals(sourceIdentity, destinationIdentity, StringComparison.Ordinal))
        {
            return;
        }

        foreach (var definition in request.Definitions)
        {
            var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, definition.SourceName);
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, definition.DestinationName);
            if (string.Equals(sourceTable, destinationTable, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Refusing to copy provider table '{definition.SourceName}' to itself. " +
                    "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
            }
        }
    }
}
