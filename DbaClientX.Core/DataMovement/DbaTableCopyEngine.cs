using System.Diagnostics;
using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>
/// Coordinates provider-neutral table-data copy operations using source and destination adapters.
/// </summary>
public sealed class DbaTableCopyEngine
{
    /// <summary>
    /// Copies one or more table definitions from a source adapter to a destination adapter.
    /// </summary>
    public async Task<DbaTableCopyResult> CopyAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        IEnumerable<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (destination == null)
        {
            throw new ArgumentNullException(nameof(destination));
        }

        if (definitions == null)
        {
            throw new ArgumentNullException(nameof(definitions));
        }

        options ??= new DbaTableCopyOptions();
        ValidateOptions(options);

        var copyDefinitions = definitions.ToArray();
        foreach (var definition in copyDefinitions)
        {
            definition.Validate();
        }

        if (options.ClearDestination)
        {
            ValidateUniqueClearDestinations(copyDefinitions);
        }

        var sw = Stopwatch.StartNew();
        var preflight = options.ClearDestination
            ? await PreflightSourceAsync(source, destination, copyDefinitions, options, cancellationToken).ConfigureAwait(false)
            : null;

        if (options.ClearDestination)
        {
            await PreflightDestinationAsync(destination, copyDefinitions, cancellationToken).ConfigureAwait(false);

            for (var index = copyDefinitions.Length - 1; index >= 0; index--)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await destination.ClearAsync(copyDefinitions[index], cancellationToken).ConfigureAwait(false);
            }
        }

        var results = new List<DbaTableCopyTableResult>();
        for (var index = 0; index < copyDefinitions.Length; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(await CopyTableAsync(
                    source,
                    destination,
                    copyDefinitions[index],
                    options,
                    preflight?[index],
                    cancellationToken)
                .ConfigureAwait(false));
        }

        sw.Stop();
        return new DbaTableCopyResult(results, sw.Elapsed);
    }

    private static async Task<DbaTableCopyPreflight?[]> PreflightSourceAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var results = new DbaTableCopyPreflight?[definitions.Count];
        var destinationPagePreflight = destination as IDbaTableCopyPagePreflightDestination;
        for (var index = 0; index < definitions.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var definition = definitions[index];
            var sourceRows = await source.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
            DataTable? firstPage = null;
            if (sourceRows != 0 || ShouldWriteEmptyPage(destination, definition))
            {
                firstPage = await source.ReadPageAsync(
                        new DbaTableCopyPageRequest(definition, 0, options.PageSize),
                        cancellationToken)
                    .ConfigureAwait(false);
                if (firstPage.Columns.Count > 0)
                {
                    PreflightTransform(firstPage, definition, destinationPagePreflight);
                }
            }

            results[index] = new DbaTableCopyPreflight(sourceRows, firstPage);
        }

        return results;
    }

    private static async Task PreflightDestinationAsync(
        IDbaTableCopyDestination destination,
        IReadOnlyList<DbaTableCopyDefinition> definitions,
        CancellationToken cancellationToken)
    {
        foreach (var definition in definitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
        }
    }

    private static void PreflightTransform(
        DataTable page,
        DbaTableCopyDefinition definition,
        IDbaTableCopyPagePreflightDestination? destinationPagePreflight)
    {
        var transformed = DbaTableCopyPageTransformer.Transform(page, definition);
        using var transformedToDispose = ReferenceEquals(transformed, page) ? null : transformed;

        if (transformed.Columns.Count == 0)
        {
            throw new InvalidOperationException(
                $"Table copy definition '{definition.DisplayName}' produced no destination columns during preflight. " +
                "At least one destination column is required before clearing destination data.");
        }

        destinationPagePreflight?.ValidatePage(definition, transformed);
    }

    private static async Task<DbaTableCopyTableResult> CopyTableAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        DbaTableCopyPreflight? preflight,
        CancellationToken cancellationToken)
    {
        var sourceRows = preflight == null
            ? await source.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false)
            : preflight.SourceRows;
        var initialDestinationRows = options.VerifyRowCounts &&
                                     !options.ClearDestination &&
                                     !ShouldWriteEmptyPage(destination, definition)
            ? await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false)
            : null;
        if (sourceRows == 0)
        {
            var emptyPage = preflight?.FirstPage;
            if (emptyPage == null && ShouldWriteEmptyPage(destination, definition))
            {
                emptyPage = await source.ReadPageAsync(
                        new DbaTableCopyPageRequest(definition, 0, options.PageSize),
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            if (emptyPage is { Columns.Count: > 0 })
            {
                await CopyPageAsync(destination, definition, options, emptyPage, 0, sourceRows, cancellationToken).ConfigureAwait(false);
            }

            return await CompleteCopyAsync(destination, definition, options, sourceRows, 0, initialDestinationRows, cancellationToken).ConfigureAwait(false);
        }

        long copied = 0;
        long offset = 0;
        var page = preflight?.FirstPage;
        if (page != null)
        {
            if (page.Rows.Count == 0)
            {
                return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, cancellationToken).ConfigureAwait(false);
            }

            var pageRows = await CopyPageAsync(destination, definition, options, page, copied, sourceRows, cancellationToken).ConfigureAwait(false);
            copied += pageRows;
            offset += page.Rows.Count;
            if (page.Rows.Count < options.PageSize)
            {
                return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, cancellationToken).ConfigureAwait(false);
            }
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            page = await source.ReadPageAsync(
                    new DbaTableCopyPageRequest(definition, offset, options.PageSize),
                    cancellationToken)
                .ConfigureAwait(false);

            if (page.Rows.Count == 0)
            {
                break;
            }

            var pageRows = await CopyPageAsync(destination, definition, options, page, copied, sourceRows, cancellationToken).ConfigureAwait(false);
            copied += pageRows;
            offset += page.Rows.Count;

            if (page.Rows.Count < options.PageSize)
            {
                break;
            }
        }

        return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<int> CopyPageAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        DataTable page,
        long previousCopied,
        long? sourceRows,
        CancellationToken cancellationToken)
    {
        var destinationPage = DbaTableCopyPageTransformer.Transform(page, definition);
        await destination.WritePageAsync(definition, destinationPage, options, cancellationToken).ConfigureAwait(false);
        options.Progress?.Invoke(new DbaTableCopyProgress(definition.DisplayName, previousCopied + destinationPage.Rows.Count, sourceRows, destinationPage.Rows.Count));
        return destinationPage.Rows.Count;
    }

    private static async Task<DbaTableCopyTableResult> CompleteCopyAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        long? sourceRows,
        long copied,
        long? initialDestinationRows,
        CancellationToken cancellationToken)
    {
        long? destinationRows = null;
        var verified = true;
        if (options.VerifyRowCounts)
        {
            destinationRows = await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
            if (sourceRows.HasValue && destinationRows.HasValue)
            {
                if (options.ClearDestination)
                {
                    verified = sourceRows.Value == destinationRows.Value;
                }
                else if (initialDestinationRows.HasValue)
                {
                    verified = copied == sourceRows.Value &&
                               destinationRows.Value == initialDestinationRows.Value + copied;
                }
            }
        }

        return new DbaTableCopyTableResult(definition.DisplayName, sourceRows, copied, destinationRows, verified);
    }

    private static bool ShouldWriteEmptyPage(IDbaTableCopyDestination destination, DbaTableCopyDefinition definition)
        => destination is IDbaTableCopyEmptyPageDestination emptyPageDestination &&
           emptyPageDestination.ShouldWriteEmptyPage(definition);

    private static void ValidateOptions(DbaTableCopyOptions options)
    {
        if (options.PageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.PageSize), "PageSize must be greater than zero.");
        }

        if (options.BatchSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BatchSize), "BatchSize must be greater than zero.");
        }

        if (options.BulkCopyTimeout is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BulkCopyTimeout), "BulkCopyTimeout must be greater than zero.");
        }
    }

    private static void ValidateUniqueClearDestinations(IReadOnlyList<DbaTableCopyDefinition> definitions)
    {
        var duplicate = definitions
            .GroupBy(static definition => NormalizeDestinationNameForDuplicateCheck(definition.DestinationName), StringComparer.Ordinal)
            .FirstOrDefault(static group => group.Count() > 1);
        if (duplicate != null)
        {
            throw new InvalidOperationException(
                $"ClearDestination cannot be used with multiple definitions targeting destination '{duplicate.First().DestinationName}'. " +
                "Each cleared destination table must be unique.");
        }
    }

    private static string NormalizeDestinationNameForDuplicateCheck(string destinationName)
        => DbaIdentifierPath.NormalizeForDuplicateCheck(destinationName);

    private sealed record DbaTableCopyPreflight(long? SourceRows, DataTable? FirstPage);
}
