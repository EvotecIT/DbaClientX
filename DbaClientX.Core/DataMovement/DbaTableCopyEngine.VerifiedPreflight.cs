using System.Data;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    private static async Task<ContentProof[]> ReadVerifiedSourceProofsAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        IDbaTableCopySchemaPreflightBatchSessionDestination? batchDestination,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken,
        CopyMeasurements? measurements)
    {
        var proofs = new ContentProof[definitions.Count];
        await using var batch = batchDestination == null ? null : await OpenVerifiedSourcePreflightAsync(
            source, batchDestination, destination as IDbaTableCopyPagePreflightDestination,
            definitions, options, cancellationToken, measurements).ConfigureAwait(false);
        for (var index = 0; index < definitions.Count; index++)
        {
            proofs[index] = await ReadContentProofAsync(source, definitions[index], options, null,
                DbaTableCopyPhase.ValidateSource, cancellationToken, destination,
                measurements: measurements, batchPreflight: batch, definitionIndex: index).ConfigureAwait(false);
        }
        // Disposal rolls back the validation transaction before checkpoint metadata or destination probes.
        return proofs;
    }

    private static async Task<VerifiedSourcePreflight> OpenVerifiedSourcePreflightAsync(
        IDbaTableCopySource source,
        IDbaTableCopySchemaPreflightBatchSessionDestination destination,
        IDbaTableCopyPagePreflightDestination? pagePreflight,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken,
        CopyMeasurements? measurements)
    {
        using var measure = measurements?.BeginPhase(DbaTableCopyPhase.PreflightSource);
        var firstPages = new DbaTableCopyPreflight?[definitions.Count];
        var projected = new DataTable?[definitions.Count];
        try
        {
            for (var index = 0; index < definitions.Count; index++)
            {
                var definition = definitions[index];
                long? rows = await CountRowsAsync(source, definition, "source", cancellationToken).ConfigureAwait(false);
                if (!rows.HasValue)
                    throw new InvalidOperationException($"Cannot verify '{definition.DisplayName}' without an exact row count.");
                int pageSize = rows > 0 ? GetReadPageSize(options.PageSize, rows, 0) : options.PageSize;
                DbaTableCopyPage page = await ReadPageAsync(source,
                    new DbaTableCopyPageRequest(definition, null, pageSize) { MaxBytes = options.MaxPageBytes },
                    1, cancellationToken, measurements: measurements).ConfigureAwait(false);
                firstPages[index] = new DbaTableCopyPreflight(rows, page, 1);
                projected[index] = DbaTableCopyPageTransformer.Transform(page.Data, definition);
                ValidateTransformedPage(projected[index]!, definition, pagePreflight);
            }
            var session = await destination.OpenSchemaPreflightBatchSessionAsync(
                definitions, projected, options, cancellationToken).ConfigureAwait(false);
            return new VerifiedSourcePreflight(firstPages, projected, session, measurements);
        }
        catch
        {
            DisposeVerifiedFirstPages(firstPages, projected);
            throw;
        }
    }

    private static void DisposeVerifiedFirstPages(
        IReadOnlyList<DbaTableCopyPreflight?> firstPages, IReadOnlyList<DataTable?> projected)
    {
        for (var index = 0; index < projected.Count; index++)
        {
            if (projected[index] is DataTable page && !ReferenceEquals(page, firstPages[index]?.FirstPage?.Data))
                page.Dispose();
        }
        DisposePreflightPages(firstPages);
    }

    private sealed class VerifiedSourcePreflight : IAsyncDisposable
    {
        private readonly IReadOnlyList<DbaTableCopyPreflight?> _firstPages;
        private readonly IReadOnlyList<DataTable?> _projected;
        private readonly CopyMeasurements? _measurements;
        private bool _disposed;
        internal IDbaTableCopySchemaPreflightBatchSession Session { get; }

        internal VerifiedSourcePreflight(IReadOnlyList<DbaTableCopyPreflight?> firstPages,
            IReadOnlyList<DataTable?> projected, IDbaTableCopySchemaPreflightBatchSession session,
            CopyMeasurements? measurements)
        {
            _firstPages = firstPages;
            _projected = projected;
            Session = session;
            _measurements = measurements;
        }

        internal long SourceRows(int index) => _firstPages[index]!.SourceRows!.Value;
        internal DbaTableCopyPage FirstPage(int index) => _firstPages[index]!.FirstPage!;
        internal DataTable ProjectedFirstPage(int index) => _projected[index]!;

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;
            using var preflight = _measurements?.BeginPhase(DbaTableCopyPhase.PreflightSource);
            try { await Session.DisposeAsync().ConfigureAwait(false); }
            finally { DisposeVerifiedFirstPages(_firstPages, _projected); }
        }
    }
}
