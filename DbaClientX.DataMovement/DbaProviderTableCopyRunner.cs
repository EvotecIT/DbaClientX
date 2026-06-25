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
        if (request.Source.Provider != request.Destination.Provider ||
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

        if (request.AllowSameProviderTableCopy)
        {
            return;
        }

        var sourceDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Source);
        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        foreach (var definition in request.Definitions)
        {
            var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, definition.SourceName, sourceDatabase, sourceDefaultSchema);
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, definition.DestinationName, destinationDatabase, destinationDefaultSchema);
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
        ValidateClearDestinationTableNamesAreUnambiguous(request);

        var sourceDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Source);
        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        var sourceTables = new HashSet<string>(
            request.Definitions.Select(definition => DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, definition.SourceName, sourceDatabase, sourceDefaultSchema)),
            StringComparer.Ordinal);

        foreach (var definition in request.Definitions)
        {
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, definition.DestinationName, destinationDatabase, destinationDefaultSchema);
            if (sourceTables.Contains(destinationTable))
            {
                throw new InvalidOperationException(
                    $"Refusing to clear destination table '{definition.DestinationName}' because it is also used as a source table in the same provider database. " +
                    "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
            }
        }
    }

    private static void ValidateClearDestinationTableNamesAreUnambiguous(DbaProviderTableCopyRequest request)
    {
        if (request.Source.Provider != DbaTableCopyProvider.SqlServer)
        {
            return;
        }

        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        foreach (var definition in request.Definitions)
        {
            if (string.IsNullOrWhiteSpace(sourceDefaultSchema) &&
                DbaProviderTableCopyTargetIdentity.IsUnqualifiedTableName(definition.SourceName))
            {
                throw CreateAmbiguousSqlServerTableException(definition.SourceName);
            }

            if (string.IsNullOrWhiteSpace(destinationDefaultSchema) &&
                DbaProviderTableCopyTargetIdentity.IsUnqualifiedTableName(definition.DestinationName))
            {
                throw CreateAmbiguousSqlServerTableException(definition.DestinationName);
            }
        }
    }

    private static InvalidOperationException CreateAmbiguousSqlServerTableException(string tableName)
        => new(
            $"Refusing to clear destination while SQL Server table '{tableName}' is unqualified and the connection default schema is unknown. " +
            "Schema-qualify SQL Server source and destination tables, provide a Current Schema connection option, or use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
}
