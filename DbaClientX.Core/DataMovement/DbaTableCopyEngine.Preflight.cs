using System.Diagnostics;
using System.Data;
using DBAClientX.Diagnostics;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    private static async Task<DbaTableCopyPreflight?[]> PreflightSourceAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var results = new DbaTableCopyPreflight?[definitions.Count];
        try
        {
            for (var index = 0; index < definitions.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var definition = definitions[index];
                var sourceRows = await CountRowsAsync(
                        source,
                        definition,
                        "source",
                        cancellationToken)
                    .ConfigureAwait(false);
                DbaTableCopyPage? firstPage = null;
                if (sourceRows != 0 || ShouldWriteEmptyPage(destination, definition))
                {
                    var pageSize = sourceRows > 0
                        ? GetReadPageSize(options.PageSize, sourceRows, copied: 0)
                        : options.PageSize;
                    firstPage = await ReadPageAsync(
                            source,
                            new DbaTableCopyPageRequest(definition, continuationToken: null, pageSize: pageSize) { MaxBytes = options.MaxPageBytes },
                            pageSequence: 1,
                            cancellationToken)
                        .ConfigureAwait(false);
                    results[index] = new DbaTableCopyPreflight(sourceRows, firstPage, pageCount: 1);
                    if (firstPage.Data.Columns.Count > 0)
                    {
                        await PreflightTransformAsync(firstPage.Data, definition, destination, options, cancellationToken).ConfigureAwait(false);
                    }
                }
                else
                {
                    results[index] = new DbaTableCopyPreflight(sourceRows, firstPage, pageCount: 0);
                }
            }

            return results;
        }
        catch
        {
            DisposePreflightPages(results);
            throw;
        }
    }

    private static async Task PreflightDestinationAsync(
        IDbaTableCopyDestination destination,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        IReadOnlyList<DbaTableCopyPreflight?> preflight,
        CancellationToken cancellationToken)
    {
        for (var index = 0; index < definitions.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var definition = definitions[index];
            var destinationRows = await CountDestinationRowsBeforeClearAsync(destination, definition, cancellationToken).ConfigureAwait(false);
            if (!destinationRows.HasValue &&
                HasNonEmptySource(preflight[index]) &&
                !ShouldWriteEmptyPage(destination, definition))
            {
                throw new InvalidOperationException(
                    $"Destination table '{definition.DestinationName}' could not be counted before ClearDestination. " +
                    "Missing destination tables are safe only for empty sources or destinations that can create the table before writing.");
            }
        }
    }

    private static bool HasNonEmptySource(DbaTableCopyPreflight? preflight)
        => preflight?.SourceRows > 0 || preflight?.FirstPage?.Data.Rows.Count > 0;

    private static async Task<long?> CountDestinationRowsBeforeClearAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        CancellationToken cancellationToken)
    {
        try
        {
            return await CountRowsAsync(
                    destination,
                    definition,
                    "destination",
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldSuppressDestinationCountFailure(destination, definition, ex))
        {
            return null;
        }
    }

    private static async Task PreflightTransformAsync(
        DataTable page,
        DbaTableCopyDefinition definition,
        IDbaTableCopyDestination destination,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var transformed = DbaTableCopyPageTransformer.Transform(page, definition);
        using var transformedToDispose = ReferenceEquals(transformed, page) ? null : transformed;

        ValidateTransformedPage(transformed, definition, destination as IDbaTableCopyPagePreflightDestination);
        if (destination is IDbaTableCopySchemaPreflightDestination schemaPreflight)
            await schemaPreflight.ValidateSchemaAsync(definition, transformed, options, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<long?> CountRowsForEmptyDestinationAsync(IDbaTableCopyDestination destination, DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        try { return await CountRowsAsync(destination, definition, "destination", cancellationToken).ConfigureAwait(false); }
        catch (Exception exception) when (ShouldSuppressDestinationCountFailure(destination, definition, exception)) { return 0; }
    }

    private static void ValidateTransformedPage(DataTable transformed, DbaTableCopyDefinition definition, IDbaTableCopyPagePreflightDestination? destinationPagePreflight)
    {
        if (transformed.Columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"Table copy definition '{definition.DisplayName}' produced no destination columns during preflight. " +
                "At least one destination column is required before clearing destination data.");
        }

        destinationPagePreflight?.ValidatePage(definition, transformed);
    }

    private static void DisposePreflightPages(IEnumerable<DbaTableCopyPreflight?>? preflight)
    {
        if (preflight == null)
        {
            return;
        }

        foreach (var item in preflight)
        {
            item?.Dispose();
        }
    }

    private sealed class DbaTableCopyPreflight : IDisposable
    {
        public DbaTableCopyPreflight(long? sourceRows, DbaTableCopyPage? firstPage, int pageCount)
        {
            SourceRows = sourceRows;
            FirstPage = firstPage;
            PageCount = pageCount;
        }

        public long? SourceRows { get; }

        public DbaTableCopyPage? FirstPage { get; private set; }

        public int PageCount { get; }

        public DbaTableCopyPage? TakeFirstPage()
        {
            var page = FirstPage;
            FirstPage = null;
            return page;
        }

        public void Dispose()
        {
            FirstPage?.Dispose();
            FirstPage = null;
        }
    }
}
