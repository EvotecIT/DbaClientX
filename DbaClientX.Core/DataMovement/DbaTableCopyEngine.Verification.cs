using System.Data;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    private sealed record ContentProof(long Rows, string Hash, IReadOnlyList<string> Columns);
    private sealed record VerifiedTablePlan(DbaTableCopyDefinition Definition, DbaTableCopyDefinition ReadDestination, ContentProof Source, DbaTableCopyCheckpoint Initial, DbaTableCopyCheckpoint? Existing);

    private static DbaTableCopyDefinition CreateDestinationReadDefinition(DbaTableCopyDefinition definition)
    {
        string Map(string name) => definition.ColumnMappings != null && definition.ColumnMappings.TryGetValue(name, out string? mapped) ? mapped : name;
        if (definition.DestinationOrderByColumns == null && definition.OrderByColumns!.Any(column => definition.ExcludedColumns?.Contains(column, StringComparer.OrdinalIgnoreCase) == true))
            throw new ArgumentException("Content-verified copies require the ordered key to be copied unchanged. Preserve identity keys or select another unique key.");
        IReadOnlyDictionary<string, DbaTableCopyColumnType>? conversions = definition.ColumnTypeConversions?
            .ToDictionary(pair => Map(pair.Key), pair => pair.Value, StringComparer.OrdinalIgnoreCase);
        return new DbaTableCopyDefinition(definition.DestinationName, definition.DestinationName,
            definition.DestinationOrderByColumns ?? definition.OrderByColumns!.Select(Map).ToArray(), definition.LogicalName, ColumnTypeConversions: conversions)
        { UseKeysetPagination = true };
    }

    private static async Task<ContentProof> ReadContentProofAsync(IDbaTableCopySource source, DbaTableCopyDefinition definition, DbaTableCopyOptions options, IReadOnlyList<string>? expectedColumns, DbaTableCopyPhase phase, CancellationToken cancellationToken)
    {
        long? counted = await CountRowsAsync(source, definition, phase == DbaTableCopyPhase.ValidateSource ? "source" : "destination", cancellationToken).ConfigureAwait(false);
        if (!counted.HasValue) throw new InvalidOperationException($"Cannot verify '{definition.DisplayName}' without an exact row count.");
        using var hasher = new DbaTableCopyContentHasher();
        IReadOnlyList<string>? columns = expectedColumns;
        string? token = null;
        long rows = 0;
        int pageNumber = 0;
        do
        {
            using DbaTableCopyPage page = await ReadPageAsync(source,
                new DbaTableCopyPageRequest(definition, token, options.PageSize) { MaxBytes = options.MaxPageBytes },
                ++pageNumber, cancellationToken).ConfigureAwait(false);
            string? previousToken = token;
            token = page.ContinuationToken;
            DataTable transformed = DbaTableCopyPageTransformer.Transform(page.Data, definition);
            using var owned = ReferenceEquals(transformed, page.Data) ? null : transformed;
            columns ??= transformed.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).ToArray();
            hasher.Add(transformed, columns, cancellationToken);
            rows = checked(rows + transformed.Rows.Count);
            options.Progress?.Invoke(new DbaTableCopyProgress(definition.DisplayName, rows, counted, transformed.Rows.Count) { Phase = phase });
            if (transformed.Rows.Count == 0 || token == null) break;
            if (token == previousToken || rows > counted.Value)
                throw new InvalidOperationException($"Source contents or continuation changed while verifying '{definition.DisplayName}'. Use a stable source snapshot.");
        } while (true);
        if (rows != counted.Value)
            throw new InvalidOperationException($"Source contents changed or the paging key is not unique for '{definition.DisplayName}'. Expected {counted.Value} rows but read {rows}.");
        return new ContentProof(rows, hasher.Hash, columns ?? Array.Empty<string>());
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

    private static async Task VerifyCommittedDestinationAsync(IDbaTableCopySource destination, VerifiedTablePlan plan, DbaTableCopyCheckpoint checkpoint, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        ContentProof actual = await ReadContentProofAsync(destination, plan.ReadDestination, options, plan.Source.Columns, DbaTableCopyPhase.VerifyDestination, cancellationToken).ConfigureAwait(false);
        if (actual.Rows != checkpoint.CopiedRows || actual.Hash != checkpoint.CopiedContentHash)
            throw new InvalidOperationException($"Destination contents no longer match the committed checkpoint for '{plan.Definition.DisplayName}'. No new rows were written.");
    }
}
