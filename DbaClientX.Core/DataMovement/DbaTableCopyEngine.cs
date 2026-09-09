using System.Diagnostics;
using System.Data;
using DBAClientX.Diagnostics;

namespace DBAClientX.DataMovement;

/// <summary>
/// Coordinates provider-neutral table-data copy operations using source and destination adapters.
/// </summary>
public sealed partial class DbaTableCopyEngine
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
        using IDisposable? readSession = source is IDbaTableCopyReadSession session
            ? await session.OpenReadSessionAsync(cancellationToken).ConfigureAwait(false)
            : null;
        if (readSession != null && ReferenceEquals(source, destination))
            throw new ArgumentException("Use separate source and destination adapters when the source holds a read session. Destination verification must observe committed writes.", nameof(destination));

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

        var startedUtc = DateTimeOffset.UtcNow;
        using var operation = DbaClientXDiagnostics.StartOperation(
            "DbaClientX.TableCopy",
            options.OperationId,
            new ActivityTagsCollection
            {
                { "dbaclientx.table_count", copyDefinitions.Length },
                { "dbaclientx.clear_destination", options.ClearDestination },
                { "dbaclientx.verify_row_counts", options.VerifyRowCounts }
            });
        var sw = Stopwatch.StartNew();
        DbaTableCopyPreflight?[]? preflight = null;
        try
        {
            List<DbaTableCopyTableResult> results;
            if (options.VerifyContent || options.CheckpointId != null)
            {
                results = await CopyVerifiedTablesAsync(source, destination, copyDefinitions, options, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                if (options.RequireEmptyDestination && !options.ClearDestination)
                {
                    foreach (DbaTableCopyDefinition definition in copyDefinitions)
                    {
                        long? rows = await CountRowsAsync(destination, definition, "destination", cancellationToken).ConfigureAwait(false);
                        if (rows != 0) throw new InvalidOperationException($"Destination table '{definition.DisplayName}' must be empty before copying.");
                    }
                }
                preflight = options.ClearDestination
                    ? await PreflightSourceAsync(source, destination, copyDefinitions, options, cancellationToken).ConfigureAwait(false)
                    : null;

                if (options.ClearDestination)
                {
                    await PreflightDestinationAsync(destination, copyDefinitions, preflight!, cancellationToken).ConfigureAwait(false);

                    for (var index = copyDefinitions.Length - 1; index >= 0; index--)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        await ClearDestinationAsync(destination, copyDefinitions[index], cancellationToken).ConfigureAwait(false);
                    }
                }

                results = new List<DbaTableCopyTableResult>();
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
            }

            sw.Stop();
            var completedUtc = DateTimeOffset.UtcNow;
            var manifest = DbaTableCopyRunManifest.Create(
                operation.OperationId,
                startedUtc,
                completedUtc,
                sw.Elapsed,
                source,
                destination,
                copyDefinitions,
                options,
                results,
                operation.Telemetry.RetryCount,
                operation.Telemetry.Warnings);
            operation.Activity?.SetTag("dbaclientx.rows_copied", results.Sum(static result => result.CopiedRows));
            operation.Activity?.SetTag("dbaclientx.retry_count", manifest.RetryCount);
            operation.Activity?.SetTag("dbaclientx.verified", manifest.Verified);
            operation.Activity?.SetStatus(ActivityStatusCode.Ok);
            return new DbaTableCopyResult(results, sw.Elapsed)
            {
                Manifest = manifest
            };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            operation.Activity?.SetTag("dbaclientx.status", "cancelled");
            operation.Activity?.AddEvent(new ActivityEvent("dbaclientx.cancelled"));
            throw;
        }
        catch (Exception ex)
        {
            DbaClientXDiagnostics.RecordException(operation.Activity, ex);
            throw;
        }
        finally
        {
            sw.Stop();
            DisposePreflightPages(preflight);
        }
    }

    private static async Task<DbaTableCopyTableResult> CopyTableAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        DbaTableCopyPreflight? preflight,
        CancellationToken cancellationToken)
    {
        using var activity = DbaClientXDiagnostics.StartActivity("DbaClientX.TableCopy.Table");
        activity?.SetTag(
            "dbaclientx.table",
            DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName));
        var sourceRows = preflight == null
            ? await CountRowsAsync(source, definition, "source", cancellationToken).ConfigureAwait(false)
            : preflight.SourceRows;
        var initialDestinationRows = await CountInitialDestinationRowsAsync(
                destination,
                definition,
                options,
                cancellationToken)
            .ConfigureAwait(false);
        var page = preflight?.TakeFirstPage();
        var pageCount = preflight?.PageCount ?? 0;
        if (sourceRows == 0)
        {
            if (page == null && ShouldWriteEmptyPage(destination, definition))
            {
                page = await ReadPageAsync(
                        source,
                        new DbaTableCopyPageRequest(definition, continuationToken: null, pageSize: options.PageSize) { MaxBytes = options.MaxPageBytes },
                        pageSequence: ++pageCount,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            if (page == null || page.Data.Columns.Count == 0)
            {
                page?.Dispose();
                return await CompleteCopyAsync(destination, definition, options, sourceRows, 0, initialDestinationRows, pageCount, cancellationToken).ConfigureAwait(false);
            }

            if (page.Data.Rows.Count == 0)
            {
                using (page)
                {
                    await CopyPageAsync(destination, definition, options, page.Data, 0, sourceRows, pageCount, cancellationToken).ConfigureAwait(false);
                }
                return await CompleteCopyAsync(destination, definition, options, sourceRows, 0, initialDestinationRows, pageCount, cancellationToken).ConfigureAwait(false);
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
                    copied += await CopyPageAsync(destination, definition, options, page.Data, copied, sourceRows, pageCount, cancellationToken).ConfigureAwait(false);
                }
            }

            if (HasCopiedKnownSourceRows(sourceRows, copied))
            {
                return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, pageCount, cancellationToken).ConfigureAwait(false);
            }

            if (continuationToken == null)
            {
                return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, pageCount, cancellationToken).ConfigureAwait(false);
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
            using var nextPage = await ReadPageAsync(
                    source,
                    new DbaTableCopyPageRequest(definition, requestedToken, pageSize) { MaxBytes = options.MaxPageBytes },
                    pageSequence: ++pageCount,
                    cancellationToken)
                .ConfigureAwait(false);

            continuationToken = nextPage.ContinuationToken;
            ValidateContinuationProgress(requestedToken, continuationToken, observedTokens, definition);

            if (nextPage.Data.Rows.Count > 0)
            {
                copied += await CopyPageAsync(destination, definition, options, nextPage.Data, copied, sourceRows, pageCount, cancellationToken).ConfigureAwait(false);
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

        return await CompleteCopyAsync(destination, definition, options, sourceRows, copied, initialDestinationRows, pageCount, cancellationToken).ConfigureAwait(false);
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
        int pageSequence,
        CancellationToken cancellationToken)
    {
        var destinationPage = DbaTableCopyPageTransformer.Transform(page, definition);
        using var destinationPageToDispose = ReferenceEquals(destinationPage, page) ? null : destinationPage;
        await WritePageAsync(
                destination,
                definition,
                destinationPage,
                options,
                pageSequence,
                cancellationToken)
            .ConfigureAwait(false);
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
        int pageCount,
        CancellationToken cancellationToken)
    {
        long? destinationRows = null;
        var verified = true;
        if (options.VerifyRowCounts)
        {
            destinationRows = await CountRowsAsync(
                    destination,
                    definition,
                    "destination",
                    cancellationToken)
                .ConfigureAwait(false);
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

        if (!sourceRows.HasValue)
        {
            DbaClientXDiagnostics.RecordWarning(
                "source_count_unknown",
                $"Source row count was unavailable for '{DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName)}'.");
        }

        if (options.VerifyRowCounts && !destinationRows.HasValue)
        {
            DbaClientXDiagnostics.RecordWarning(
                "destination_count_unknown",
                $"Destination row count was unavailable for '{DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName)}'.");
        }

        if (!verified)
        {
            DbaClientXDiagnostics.RecordWarning(
                "row_count_verification_failed",
                $"Row-count verification failed for '{DbaClientXDiagnostics.SanitizeLogicalName(definition.DisplayName)}'.");
        }

        return new DbaTableCopyTableResult(definition.DisplayName, sourceRows, copied, destinationRows, verified)
        {
            PageCount = pageCount
        };
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
        if (options.MaxPageBytes is <= 0) throw new ArgumentOutOfRangeException(nameof(options.MaxPageBytes));
        if (options.CheckpointId != null && (string.IsNullOrWhiteSpace(options.CheckpointId) || options.CheckpointId.Length > 128))
            throw new ArgumentException("CheckpointId must contain between 1 and 128 non-whitespace characters.", nameof(options.CheckpointId));
        if (options.Resume && (options.CheckpointId == null || options.ClearDestination))
            throw new ArgumentException("Resume requires CheckpointId and cannot be combined with ClearDestination.");
        if (options.PageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.PageSize), "PageSize must be greater than zero.");
        }

        if (options.BatchSize is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BatchSize), "BatchSize must be greater than zero.");
        }

        if (options.BulkCopyTimeout is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.BulkCopyTimeout), "BulkCopyTimeout cannot be negative.");
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

}
