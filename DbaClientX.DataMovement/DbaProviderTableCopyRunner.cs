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

        if (request.Options?.ClearDestination == true)
        {
            ValidateClearDestinationDoesNotRemoveSources(request);
        }

        var sourceDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Source);
        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        foreach (var definition in request.Definitions)
        {
            var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, definition.SourceName, sourceDatabase);
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, definition.DestinationName, destinationDatabase);
            if (string.Equals(sourceTable, destinationTable, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Refusing to copy provider table '{definition.SourceName}' to itself. " +
                    "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
            }
        }
    }

    private static void ValidateClearDestinationDoesNotRemoveSources(DbaProviderTableCopyRequest request)
    {
        var sourceDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Source);
        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        var sourceTables = new HashSet<string>(
            request.Definitions.Select(definition => DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, definition.SourceName, sourceDatabase)),
            StringComparer.Ordinal);

        foreach (var definition in request.Definitions)
        {
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, definition.DestinationName, destinationDatabase);
            if (sourceTables.Contains(destinationTable))
            {
                throw new InvalidOperationException(
                    $"Refusing to clear destination table '{definition.DestinationName}' because it is also used as a source table in the same provider database. " +
                    "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
            }
        }
    }
}
