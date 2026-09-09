using System.Data;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    private static async Task<List<DbaTableCopyTableResult>> CopyVerifiedTablesAsync(IDbaTableCopySource source, IDbaTableCopyDestination destination, IReadOnlyList<DbaTableCopyDefinition> definitions, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        if (destination is not IDbaTableCopySource destinationReader)
            throw new NotSupportedException("Content verification requires a destination that can read back copied rows.");
        IDbaTableCopyCheckpointDestination? checkpoints = null;
        if (options.CheckpointId != null)
        {
            checkpoints = destination as IDbaTableCopyCheckpointDestination;
            if (checkpoints?.SupportsAtomicCheckpoints != true)
                throw new NotSupportedException("The destination does not support atomic page checkpoints.");
        }
        ValidateUniqueClearDestinations(definitions);
        if (definitions.Any(static definition => !definition.UseKeysetPagination))
            throw new ArgumentException("Content-verified and resumable copies require keyset pagination for every table.");

        var plans = new List<VerifiedTablePlan>();
        using var emptyHasher = new DbaTableCopyContentHasher();
        foreach (DbaTableCopyDefinition definition in definitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            DbaTableCopyDefinition destinationDefinition = CreateDestinationReadDefinition(definition);
            ContentProof proof = await ReadContentProofAsync(source, definition, options, null, DbaTableCopyPhase.ValidateSource, cancellationToken).ConfigureAwait(false);
            string fingerprint = DbaTableCopyRunManifest.ComputeDefinitionFingerprint(new[] { definition }, new DbaTableCopyOptions { KeepIdentity = options.KeepIdentity });
            var initial = new DbaTableCopyCheckpoint
            {
                CopyId = options.CheckpointId ?? "transient", DefinitionFingerprint = fingerprint,
                SourceRows = proof.Rows, SourceContentHash = proof.Hash, CopiedContentHash = emptyHasher.Hash
            };
            DbaTableCopyCheckpoint? existing = checkpoints == null ? null : await checkpoints.ReadCheckpointAsync(definition, cancellationToken).ConfigureAwait(false);
            if (existing != null && existing.CopyId == initial.CopyId)
            {
                if (!options.Resume) throw new InvalidOperationException("This copy already has checkpoints. Use Resume, or choose a new identifier for an explicit restart.");
                ValidateCheckpointSource(existing, initial, definition.DisplayName);
            }
            else if (options.Resume && existing != null)
            {
                throw new InvalidOperationException($"Destination table '{definition.DisplayName}' belongs to another copy identifier.");
            }
            else
            {
                existing = null;
            }
            var plan = new VerifiedTablePlan(definition, destinationDefinition, proof, initial, existing);
            if (existing != null)
            {
                await VerifyCommittedDestinationAsync(destinationReader, plan, existing, options, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                long? rows = await CountRowsAsync(destination, definition, "destination", cancellationToken).ConfigureAwait(false);
                if (!rows.HasValue || (rows != 0 && !options.ClearDestination))
                    throw new InvalidOperationException($"Destination table '{definition.DisplayName}' must exist and be empty, or explicitly overwritten, before a verified copy.");
            }
            plans.Add(plan);
        }
        // A process can stop during preflight, before the first checkpoint is initialized.
        // Resume may initialize missing checkpoints only after proving those destinations empty above.

        // Finish all source and destination checks before clearing any table. Reverse order respects dependencies.
        for (int index = plans.Count - 1; index >= 0; index--)
        {
            VerifiedTablePlan plan = plans[index];
            if (plan.Existing != null) continue;
            if (checkpoints != null)
                await checkpoints.InitializeCheckpointAsync(plan.Definition, plan.Initial, options.ClearDestination, cancellationToken).ConfigureAwait(false);
            else if (options.ClearDestination)
                await ClearDestinationAsync(destination, plan.Definition, cancellationToken).ConfigureAwait(false);
        }

        var results = new List<DbaTableCopyTableResult>();
        foreach (VerifiedTablePlan plan in plans)
        {
            results.Add(await CopyVerifiedTableAsync(source, destination, destinationReader, checkpoints, plan, options, cancellationToken).ConfigureAwait(false));
        }
        return results;
    }

    private static async Task<DbaTableCopyTableResult> CopyVerifiedTableAsync(IDbaTableCopySource source, IDbaTableCopyDestination destination, IDbaTableCopySource destinationReader, IDbaTableCopyCheckpointDestination? checkpoints, VerifiedTablePlan plan, DbaTableCopyOptions options, CancellationToken cancellationToken)
    {
        DbaTableCopyCheckpoint current = plan.Existing ?? plan.Initial;
        long resumedRows = current.CopiedRows;
        int pageCount = 0;
        using var hasher = new DbaTableCopyContentHasher(current.CopiedContentHash);
        while (current.CopiedRows < plan.Source.Rows)
        {
            using DbaTableCopyPage page = await ReadPageAsync(source,
                new DbaTableCopyPageRequest(plan.Definition, current.ContinuationToken, options.PageSize) { MaxBytes = options.MaxPageBytes },
                ++pageCount, cancellationToken).ConfigureAwait(false);
            if (page.Data.Rows.Count == 0 || page.ContinuationToken == current.ContinuationToken)
                throw new InvalidOperationException($"Source ended or stopped advancing before all rows of '{plan.Definition.DisplayName}' were copied.");
            DataTable transformed = DbaTableCopyPageTransformer.Transform(page.Data, plan.Definition);
            using var owned = ReferenceEquals(transformed, page.Data) ? null : transformed;
            hasher.Add(transformed, plan.Source.Columns, cancellationToken);
            var next = current with
            {
                CopiedRows = checked(current.CopiedRows + transformed.Rows.Count),
                ContinuationToken = page.ContinuationToken, CopiedContentHash = hasher.Hash
            };
            if (next.CopiedRows > plan.Source.Rows)
                throw new InvalidOperationException($"Source gained rows while copying '{plan.Definition.DisplayName}'. Use a stable source snapshot.");
            if (checkpoints != null)
                await checkpoints.CommitPageAsync(plan.Definition, transformed, options, current, next, cancellationToken).ConfigureAwait(false);
            else
                await WritePageAsync(destination, plan.Definition, transformed, options, pageCount, cancellationToken).ConfigureAwait(false);
            current = next;
            options.Progress?.Invoke(new DbaTableCopyProgress(plan.Definition.DisplayName, current.CopiedRows, plan.Source.Rows, transformed.Rows.Count));
        }
        if (current.CopiedRows != plan.Source.Rows || current.CopiedContentHash != plan.Source.Hash)
            throw new InvalidOperationException($"Source contents changed during the copy of '{plan.Definition.DisplayName}'. The migration is not verified.");
        ContentProof actual = await ReadContentProofAsync(destinationReader, plan.ReadDestination, options, plan.Source.Columns, DbaTableCopyPhase.VerifyDestination, cancellationToken).ConfigureAwait(false);
        bool verified = actual.Rows == plan.Source.Rows && actual.Hash == plan.Source.Hash;
        if (verified && checkpoints != null && !current.Completed)
        {
            using var empty = new DataTable();
            await checkpoints.CommitPageAsync(plan.Definition, empty, options, current, current with { Completed = true }, cancellationToken).ConfigureAwait(false);
        }
        return new DbaTableCopyTableResult(plan.Definition.DisplayName, plan.Source.Rows, current.CopiedRows, actual.Rows, verified)
        {
            SourceContentHash = plan.Source.Hash, DestinationContentHash = actual.Hash,
            ResumedRows = resumedRows, PageCount = pageCount
        };
    }
}
