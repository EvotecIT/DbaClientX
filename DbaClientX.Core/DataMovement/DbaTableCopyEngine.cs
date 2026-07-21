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
            if (destination is IDbaTableCopyClearSafetyValidator clearSafetyValidator)
            {
                clearSafetyValidator.ValidateClearOperation(source, copyDefinitions);
            }
        }

        var sw = Stopwatch.StartNew();
        var preflight = options.ClearDestination
            ? await PreflightSourceAsync(source, destination, copyDefinitions, options, cancellationToken).ConfigureAwait(false)
            : null;

        try
        {
            if (options.ClearDestination)
            {
                await PreflightDestinationAsync(destination, copyDefinitions, preflight!, cancellationToken).ConfigureAwait(false);

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
        finally
        {
            DisposePreflightPages(preflight);
        }
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
        try
        {
            for (var index = 0; index < definitions.Count; index++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var definition = definitions[index];
                var sourceRows = await source.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
                DbaTableCopyPage? firstPage = null;
                if (sourceRows != 0 || ShouldWriteEmptyPage(destination, definition))
                {
                    var pageSize = sourceRows > 0
                        ? GetReadPageSize(options.PageSize, sourceRows, copied: 0)
                        : options.PageSize;
                    firstPage = await source.ReadPageAsync(
                            new DbaTableCopyPageRequest(definition, continuationToken: null, pageSize: pageSize),
                        cancellationToken)
                        .ConfigureAwait(false);
                    results[index] = new DbaTableCopyPreflight(sourceRows, firstPage);
                    if (firstPage.Data.Columns.Count > 0)
                    {
                        PreflightTransform(firstPage.Data, definition, destinationPagePreflight);
                    }
                }
                else
                {
                    results[index] = new DbaTableCopyPreflight(sourceRows, firstPage);
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
            return await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldSuppressDestinationCountFailure(destination, definition, ex))
        {
            return null;
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
        var initialDestinationRows = await CountInitialDestinationRowsAsync(
                destination,
                definition,
                options,
                cancellationToken)
            .ConfigureAwait(false);
        var page = preflight?.TakeFirstPage();
        if (sourceRows == 0)
        {
            if (page == null && ShouldWriteEmptyPage(destination, definition))
            {
                page = await source.ReadPageAsync(
                        new DbaTableCopyPageRequest(definition, continuationToken: null, pageSize: options.PageSize),
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            if (page == null || page.Data.Columns.Count == 0)
            {
                page?.Dispose();
                return await CompleteCopyAsync(destination, definition, options, sourceRows, 0, initialDestinationRows, cancellationToken).ConfigureAwait(false);
            }

            if (page.Data.Rows.Count == 0)
            {
                using (page)
                {
                    await CopyPageAsync(destination, definition, options, page.Data, 0, sourceRows, cancellationToken).ConfigureAwait(false);
                }
                return await CompleteCopyAsync(destination, definition, options, sourceRows, 0, initialDestinationRows, cancellationToken).ConfigureAwait(false);
            }

            sourceRows = null;
        }

        long copied = 0;
        string? continuationToken = null;
        var observedTokens = new HashSet<string>(StringComparer.Ordinal);
        if (page != null)
        {
            continuationToken = page.ContinuationToken;
            if (continuationToken != null)
            {
                observedTokens.Add(continuationToken);
            }

            using (page)
            {
                if (page.Data.Rows.Count > 0)
                {
                    copied += await CopyPageAsync(destination, definition, options, page.Data, copied, sourceRows, cancellationToken).ConfigureAwait(false);
                }
            }

            if (HasCopiedKnownSourceRows(sourceRows, copied))
            {
                return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, cancellationToken).ConfigureAwait(false);
            }

            if (continuationToken == null)
            {
                return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, cancellationToken).ConfigureAwait(false);
            }
        }

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var pageSize = GetReadPageSize(options.PageSize, sourceRows, copied);
            if (pageSize == 0)
            {
                break;
            }

            string? requestedToken = continuationToken;
            using var nextPage = await source.ReadPageAsync(
                    new DbaTableCopyPageRequest(definition, requestedToken, pageSize),
                    cancellationToken)
                .ConfigureAwait(false);

            continuationToken = nextPage.ContinuationToken;
            ValidateContinuationProgress(requestedToken, continuationToken, observedTokens, definition);

            if (nextPage.Data.Rows.Count > 0)
            {
                copied += await CopyPageAsync(destination, definition, options, nextPage.Data, copied, sourceRows, cancellationToken).ConfigureAwait(false);
            }

            if (HasCopiedKnownSourceRows(sourceRows, copied))
            {
                break;
            }

            if (continuationToken == null)
            {
                break;
            }
        }

        return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, cancellationToken).ConfigureAwait(false);
    }

    private static int GetReadPageSize(int pageSize, long? sourceRows, long copied)
    {
        if (!sourceRows.HasValue)
        {
            return pageSize;
        }

        var remaining = sourceRows.Value - copied;
        if (remaining <= 0)
        {
            return 0;
        }

        return remaining > pageSize ? pageSize : (int)remaining;
    }

    private static bool HasCopiedKnownSourceRows(long? sourceRows, long copied)
        => sourceRows.HasValue && copied >= sourceRows.Value;

    private static void ValidateContinuationProgress(
        string? requestedToken,
        string? returnedToken,
        ISet<string> observedTokens,
        DbaTableCopyDefinition definition)
    {
        if (returnedToken == null)
        {
            return;
        }

        if (string.Equals(requestedToken, returnedToken, StringComparison.Ordinal) || !observedTokens.Add(returnedToken))
        {
            throw new InvalidOperationException(
                $"Source page stream for '{definition.DisplayName}' returned a repeated continuation token and cannot make progress.");
        }
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
        using var destinationPageToDispose = ReferenceEquals(destinationPage, page) ? null : destinationPage;
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
            if (destinationRows.HasValue)
            {
                if (options.ClearDestination)
                {
                    verified = sourceRows.HasValue
                        ? copied == sourceRows.Value && sourceRows.Value == destinationRows.Value
                        : copied == destinationRows.Value;
                }
                else if (initialDestinationRows.HasValue)
                {
                    verified = (!sourceRows.HasValue || copied == sourceRows.Value) &&
                               destinationRows.Value == initialDestinationRows.Value + copied;
                }
                else
                {
                    verified = (!sourceRows.HasValue || copied == sourceRows.Value) &&
                               destinationRows.Value == copied;
                }
            }
        }

        return new DbaTableCopyTableResult(definition.DisplayName, sourceRows, copied, destinationRows, verified);
    }

    private static async Task<long?> CountInitialDestinationRowsAsync(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        if (!options.VerifyRowCounts || options.ClearDestination)
        {
            return null;
        }

        try
        {
            return await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ShouldSuppressDestinationCountFailure(destination, definition, ex))
        {
            return null;
        }
    }

    private static bool ShouldWriteEmptyPage(IDbaTableCopyDestination destination, DbaTableCopyDefinition definition)
        => destination is IDbaTableCopyEmptyPageDestination emptyPageDestination &&
           emptyPageDestination.ShouldWriteEmptyPage(definition);

    private static bool ShouldSuppressDestinationCountFailure(
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        Exception exception)
        => ShouldWriteEmptyPage(destination, definition) &&
           destination is IDbaTableCopyMissingTableClassifier missingTableClassifier &&
           missingTableClassifier.IsMissingTableException(exception);

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
        => string.Join(".", DbaIdentifierPath.SplitSegments(destinationName).Select(NormalizeSegmentForDuplicateCheck));

    private static string NormalizeSegmentForDuplicateCheck(string segment)
    {
        var trimmed = segment.Trim();
        if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[trimmed.Length - 1] == '"')
        {
            return "\"" + trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\"").Replace("\"", "\"\"") + "\"";
        }

        return DbaIdentifierPath.UnquoteSegment(trimmed);
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
        public DbaTableCopyPreflight(long? sourceRows, DbaTableCopyPage? firstPage)
        {
            SourceRows = sourceRows;
            FirstPage = firstPage;
        }

        public long? SourceRows { get; }

        public DbaTableCopyPage? FirstPage { get; private set; }

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
