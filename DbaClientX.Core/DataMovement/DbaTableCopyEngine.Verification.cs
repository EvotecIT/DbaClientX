using System.Data;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    private sealed record ContentProof(long Rows, string Hash, IReadOnlyList<string> Columns, DbaTableCopyPageTransformer.DestinationReadProjection? DestinationProjection);
    private sealed record VerifiedTablePlan(DbaTableCopyDefinition Definition, DbaTableCopyDefinition ReadDestination, ContentProof Source, DbaTableCopyCheckpoint Initial, DbaTableCopyCheckpoint? Existing)
    {
        internal ContentProof? CommittedDestinationProof { get; set; }
    }

    private static DbaTableCopyDefinition CreateDestinationReadDefinition(DbaTableCopyDefinition definition, ContentProof proof)
    {
        return new DbaTableCopyDefinition(definition.DestinationName, definition.DestinationName,
            proof.DestinationProjection!.Keys, definition.LogicalName, ColumnTypeConversions: proof.DestinationProjection.Conversions)
        { UseKeysetPagination = true };
    }

    private static async Task<ContentProof> ReadContentProofAsync(IDbaTableCopySource source, DbaTableCopyDefinition definition, DbaTableCopyOptions options, IReadOnlyList<string>? expectedColumns, DbaTableCopyPhase phase, CancellationToken cancellationToken, IDbaTableCopyDestination? preflightDestination = null, CopyMeasurements? measurements = null, VerifiedSourcePreflight? batchPreflight = null, int definitionIndex = 0)
    {
        using var measure = measurements?.BeginPhase(phase, definition.DisplayName);
        long? counted = batchPreflight?.SourceRows(definitionIndex) ?? await CountRowsAsync(source, definition, phase == DbaTableCopyPhase.ValidateSource ? "source" : "destination", cancellationToken).ConfigureAwait(false);
        if (!counted.HasValue) throw new InvalidOperationException($"Cannot verify '{definition.DisplayName}' without an exact row count.");
        using var hasher = new DbaTableCopyContentHasher();
        IReadOnlyList<string>? columns = expectedColumns;
        DbaTableCopyPageTransformer.DestinationReadProjection? destinationProjection = null;
        string? token = null;
        long rows = 0;
        int pageNumber = 0;
        IDbaTableCopySchemaPreflightSession? schemaSession = null;
        try
        {
            do
            {
                bool prefetched = batchPreflight != null && pageNumber == 0;
                pageNumber++;
                DbaTableCopyPage page = prefetched ? batchPreflight!.FirstPage(definitionIndex) : await ReadPageAsync(source,
                    new DbaTableCopyPageRequest(definition, token, options.PageSize) { MaxBytes = options.MaxPageBytes },
                    pageNumber, cancellationToken, measurements: measurements,
                    destinationRead: phase == DbaTableCopyPhase.VerifyDestination).ConfigureAwait(false);
                using var pageToDispose = prefetched ? null : page;
                string? previousToken = token;
                token = page.ContinuationToken;
                if (phase == DbaTableCopyPhase.ValidateSource && pageNumber == 1)
                    destinationProjection = DbaTableCopyPageTransformer.ResolveDestinationReadProjection(page.Data, definition);
                DataTable transformed = phase == DbaTableCopyPhase.VerifyDestination
                    ? DbaTableCopyPageTransformer.TransformReadback(page.Data, definition)
                    : prefetched ? batchPreflight!.ProjectedFirstPage(definitionIndex)
                    : DbaTableCopyPageTransformer.Transform(page.Data, definition);
                using var owned = prefetched || ReferenceEquals(transformed, page.Data) ? null : transformed;
                if (phase == DbaTableCopyPhase.ValidateSource && page.Data.Columns.Count > 0)
                {
                    using var preflight = measurements?.BeginPhase(DbaTableCopyPhase.PreflightSource, definition.DisplayName);
                    ValidateTransformedPage(transformed, definition, preflightDestination as IDbaTableCopyPagePreflightDestination);
                    if (batchPreflight != null)
                    {
                        await batchPreflight.Session.ValidatePageAsync(definitionIndex, transformed, cancellationToken).ConfigureAwait(false);
                    }
                    else if (options.ClearDestination && preflightDestination is IDbaTableCopySchemaPreflightSessionDestination sessionDestination)
                    {
                        if (schemaSession == null)
                        {
                            schemaSession = await sessionDestination
                                .OpenSchemaPreflightSessionAsync(definition, transformed, options, cancellationToken)
                                .ConfigureAwait(false);
                        }
                        else
                        {
                            await schemaSession.ValidatePageAsync(transformed, cancellationToken).ConfigureAwait(false);
                        }
                    }
                    else if (pageNumber == 1 && preflightDestination is IDbaTableCopySchemaPreflightDestination schemaPreflight)
                    {
                        await schemaPreflight.ValidateSchemaAsync(definition, transformed, options, cancellationToken).ConfigureAwait(false);
                    }
                }
                columns ??= transformed.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray();
                hasher.Add(
                    transformed,
                    columns,
                    cancellationToken,
                    source as IDbaTableCopyContentValueNormalizer);
                rows = checked(rows + transformed.Rows.Count);
                measurements?.ReportProgress(definition.DisplayName, rows, counted, transformed.Rows.Count, rows);
                if (transformed.Rows.Count == 0 || token == null) break;
                if (token == previousToken || rows > counted.Value)
                    throw new InvalidOperationException($"Source contents or continuation changed while verifying '{definition.DisplayName}'. Use a stable source snapshot.");
            } while (true);
        }
        finally
        {
            if (schemaSession != null)
            {
                using var preflight = measurements?.BeginPhase(DbaTableCopyPhase.PreflightSource, definition.DisplayName);
                await schemaSession.DisposeAsync().ConfigureAwait(false);
            }
        }
        if (rows != counted.Value)
            throw new InvalidOperationException($"Source contents changed or the paging key is not unique for '{definition.DisplayName}'. Expected {counted.Value} rows but read {rows}.");
        return new ContentProof(rows, hasher.Hash, columns ?? Array.Empty<string>(), destinationProjection);
    }

    private static void ValidateCheckpointSource(DbaTableCopyCheckpoint checkpoint, DbaTableCopyCheckpoint expected, string table)
    {
        if (checkpoint.CopyId != expected.CopyId || checkpoint.DefinitionFingerprint != expected.DefinitionFingerprint ||
            checkpoint.SourceRows != expected.SourceRows || checkpoint.SourceContentHash != expected.SourceContentHash ||
            checkpoint.CopiedRows < 0 || checkpoint.CopiedRows > checkpoint.SourceRows ||
            (checkpoint.CopiedRows > 0 && checkpoint.ContinuationToken == null) ||
            (checkpoint.Completed && checkpoint.CopiedRows != checkpoint.SourceRows))
            throw new InvalidOperationException($"Checkpoint source or copy contract no longer matches '{table}'. No destination data was changed. Restore the original source snapshot or start a new copy.");
    }

    private static async Task<ContentProof> VerifyCommittedDestinationAsync(IDbaTableCopySource destination, VerifiedTablePlan plan, DbaTableCopyCheckpoint checkpoint, DbaTableCopyOptions options, CancellationToken cancellationToken, CopyMeasurements? measurements = null)
    {
        ContentProof actual = await ReadContentProofAsync(destination, plan.ReadDestination, options, plan.Source.Columns, DbaTableCopyPhase.VerifyDestination, cancellationToken, measurements: measurements).ConfigureAwait(false);
        if (actual.Rows != checkpoint.CopiedRows || actual.Hash != checkpoint.CopiedContentHash)
            throw new InvalidOperationException($"Destination contents no longer match the committed checkpoint for '{plan.Definition.DisplayName}'. No new rows were written.");
        return actual;
    }
}
