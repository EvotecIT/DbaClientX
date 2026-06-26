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
        ValidateClearDestinationDefinitionsAreUnique(request);

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
            !DbaProviderTableCopyTargetIdentity.TryCreate(request.Destination, out var destinationIdentity))
        {
            return;
        }

        if (!string.Equals(sourceIdentity, destinationIdentity, StringComparison.Ordinal))
        {
            ValidateExplicitCrossDatabaseSelfCopies(request);

            if (request.Options?.ClearDestination == true)
            {
                ValidateClearDestinationDoesNotRemoveExplicitCrossDatabaseSources(request);
            }

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
            var namesCanOverlap = DbaProviderTableCopyTargetIdentity.TableNamesCanReferToSameObject(
                request.Source.Provider,
                definition.SourceName,
                definition.DestinationName);
            if (namesCanOverlap)
            {
                ValidateSqlServerTableNameIsUnambiguous(request.Source, definition.SourceName, sourceDefaultSchema);
                ValidateSqlServerTableNameIsUnambiguous(request.Destination, definition.DestinationName, destinationDefaultSchema);
                ValidatePostgreSqlTableNameIsUnambiguous(request.Source, definition.SourceName);
                ValidatePostgreSqlTableNameIsUnambiguous(request.Destination, definition.DestinationName);
            }

            var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, definition.SourceName, sourceDatabase, sourceDefaultSchema);
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, definition.DestinationName, destinationDatabase, destinationDefaultSchema);
            if (namesCanOverlap &&
                string.Equals(sourceTable, destinationTable, StringComparison.Ordinal))
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
        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);

        foreach (var destinationDefinition in request.Definitions)
        {
            foreach (var sourceDefinition in request.Definitions)
            {
                if (!DbaProviderTableCopyTargetIdentity.TableNamesCanReferToSameObject(
                        request.Source.Provider,
                        sourceDefinition.SourceName,
                        destinationDefinition.DestinationName))
                {
                    continue;
                }

                ValidateClearDestinationTableNameIsUnambiguous(request.Source, sourceDefinition.SourceName, sourceDefaultSchema);
                ValidateClearDestinationTableNameIsUnambiguous(request.Destination, destinationDefinition.DestinationName, destinationDefaultSchema);

                var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Source.Provider, sourceDefinition.SourceName, sourceDatabase, sourceDefaultSchema);
                var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeTableName(request.Destination.Provider, destinationDefinition.DestinationName, destinationDatabase, destinationDefaultSchema);
                if (string.Equals(sourceTable, destinationTable, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Refusing to clear destination table '{destinationDefinition.DestinationName}' because it is also used as a source table in the same provider database. " +
                        "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
                }
            }
        }
    }

    private static void ValidateClearDestinationDoesNotRemoveExplicitCrossDatabaseSources(DbaProviderTableCopyRequest request)
    {
        if (request.Source.Provider is not (DbaTableCopyProvider.SqlServer or DbaTableCopyProvider.MySql))
        {
            return;
        }

        var sourceDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Source);
        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        foreach (var destinationDefinition in request.Definitions)
        {
            foreach (var sourceDefinition in request.Definitions)
            {
                if (!DbaProviderTableCopyTargetIdentity.TableNamesCanReferToSameObject(
                        request.Source.Provider,
                        sourceDefinition.SourceName,
                        destinationDefinition.DestinationName))
                {
                    continue;
                }

                ValidateSqlServerTableNameIsUnambiguous(request.Source, sourceDefinition.SourceName, sourceDefaultSchema);
                ValidateSqlServerTableNameIsUnambiguous(request.Destination, destinationDefinition.DestinationName, destinationDefaultSchema);

                var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeEffectiveTableTarget(request.Source.Provider, sourceDefinition.SourceName, sourceDatabase, sourceDefaultSchema);
                var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeEffectiveTableTarget(request.Destination.Provider, destinationDefinition.DestinationName, destinationDatabase, destinationDefaultSchema);
                if (string.Equals(sourceTable, destinationTable, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Refusing to clear destination table '{destinationDefinition.DestinationName}' because it is also used as a source table in the same provider database. " +
                        "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
                }
            }
        }
    }

    private static void ValidateClearDestinationTableNamesAreUnambiguous(DbaProviderTableCopyRequest request)
    {
        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        foreach (var definition in request.Definitions)
        {
            ValidateClearDestinationTableNameIsUnambiguous(request.Source, definition.SourceName, sourceDefaultSchema);
            ValidateClearDestinationTableNameIsUnambiguous(request.Destination, definition.DestinationName, destinationDefaultSchema);
        }
    }

    private static InvalidOperationException CreateAmbiguousSqlServerTableException(string tableName)
        => new(
            $"Refusing to guard SQL Server table '{tableName}' because the table name is unqualified and the connection default schema is unknown. " +
            "Schema-qualify SQL Server source and destination tables, or use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");

    private static InvalidOperationException CreateAmbiguousPostgreSqlTableException(string tableName)
        => new(
            $"Refusing to guard PostgreSQL table '{tableName}' because the connection omits Search Path and the table name is unqualified. " +
            "Schema-qualify PostgreSQL source and destination tables or provide an explicit Search Path.");

    private static void ValidateClearDestinationTableNameIsUnambiguous(DbaProviderTableCopyAdapterOptions options, string tableName, string? defaultSchema)
    {
        if (!DbaProviderTableCopyTargetIdentity.IsUnqualifiedTableName(tableName))
        {
            return;
        }

        ValidateSqlServerTableNameIsUnambiguous(options, tableName, defaultSchema);

        ValidatePostgreSqlTableNameIsUnambiguous(options, tableName);
    }

    private static void ValidateSqlServerTableNameIsUnambiguous(DbaProviderTableCopyAdapterOptions options, string tableName, string? defaultSchema)
    {
        if (options.Provider == DbaTableCopyProvider.SqlServer &&
            DbaProviderTableCopyTargetIdentity.IsUnqualifiedTableName(tableName) &&
            string.IsNullOrWhiteSpace(defaultSchema))
        {
            throw CreateAmbiguousSqlServerTableException(tableName);
        }
    }

    private static void ValidatePostgreSqlTableNameIsUnambiguous(DbaProviderTableCopyAdapterOptions options, string tableName)
    {
        if (options.Provider == DbaTableCopyProvider.PostgreSql &&
            DbaProviderTableCopyTargetIdentity.IsUnqualifiedTableName(tableName) &&
            DbaProviderTableCopyTargetIdentity.HasAmbiguousPostgreSqlDefaultSchema(options))
        {
            throw CreateAmbiguousPostgreSqlTableException(tableName);
        }
    }

    private static void ValidateExplicitCrossDatabaseSelfCopies(DbaProviderTableCopyRequest request)
    {
        if (request.AllowSameProviderTableCopy ||
            request.Source.Provider is not (DbaTableCopyProvider.SqlServer or DbaTableCopyProvider.MySql))
        {
            return;
        }

        var sourceDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Source);
        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        var sourceDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Source);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        foreach (var definition in request.Definitions)
        {
            if (!DbaProviderTableCopyTargetIdentity.TableNamesCanReferToSameObject(
                    request.Source.Provider,
                    definition.SourceName,
                    definition.DestinationName))
            {
                continue;
            }

            ValidateSqlServerTableNameIsUnambiguous(request.Source, definition.SourceName, sourceDefaultSchema);
            ValidateSqlServerTableNameIsUnambiguous(request.Destination, definition.DestinationName, destinationDefaultSchema);

            var sourceTable = DbaProviderTableCopyTargetIdentity.NormalizeEffectiveTableTarget(request.Source.Provider, definition.SourceName, sourceDatabase, sourceDefaultSchema);
            var destinationTable = DbaProviderTableCopyTargetIdentity.NormalizeEffectiveTableTarget(request.Destination.Provider, definition.DestinationName, destinationDatabase, destinationDefaultSchema);
            if (string.Equals(sourceTable, destinationTable, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"Refusing to copy provider table '{definition.SourceName}' to itself. " +
                    "Use AllowSameProviderTableCopy only when the caller intentionally owns that behavior.");
            }
        }
    }

    private static void ValidateClearDestinationDefinitionsAreUnique(DbaProviderTableCopyRequest request)
    {
        if (request.Options?.ClearDestination != true)
        {
            return;
        }

        var destinationDatabase = DbaProviderTableCopyTargetIdentity.GetCurrentDatabase(request.Destination);
        var destinationDefaultSchema = DbaProviderTableCopyTargetIdentity.GetDefaultSchema(request.Destination);
        for (var index = 0; index < request.Definitions.Count; index++)
        {
            for (var next = index + 1; next < request.Definitions.Count; next++)
            {
                var current = request.Definitions[index];
                var candidate = request.Definitions[next];
                if (!DbaProviderTableCopyTargetIdentity.TableNamesCanReferToSameObject(
                        request.Destination.Provider,
                        current.DestinationName,
                        candidate.DestinationName))
                {
                    continue;
                }

                ValidateClearDestinationTableNameIsUnambiguous(request.Destination, current.DestinationName, destinationDefaultSchema);
                ValidateClearDestinationTableNameIsUnambiguous(request.Destination, candidate.DestinationName, destinationDefaultSchema);
            }
        }

        var duplicate = request.Definitions
            .GroupBy(
                definition => DbaProviderTableCopyTargetIdentity.NormalizeTableName(
                    request.Destination.Provider,
                    definition.DestinationName,
                    destinationDatabase,
                    destinationDefaultSchema),
                StringComparer.Ordinal)
            .FirstOrDefault(static group => group.Count() > 1);
        if (duplicate != null)
        {
            throw new InvalidOperationException(
                $"ClearDestination cannot be used with multiple definitions targeting destination '{duplicate.First().DestinationName}'. " +
                "Each cleared destination table must be unique.");
        }
    }
}
