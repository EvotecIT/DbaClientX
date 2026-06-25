using System.Diagnostics;

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

        var sw = Stopwatch.StartNew();
        var results = new List<DbaTableCopyTableResult>();
        foreach (var definition in definitions)
        {
            cancellationToken.ThrowIfCancellationRequested();
            definition.Validate();
            results.Add(await CopyTableAsync(source, destination, definition, options, cancellationToken).ConfigureAwait(false));
        }

        sw.Stop();
        return new DbaTableCopyResult(results, sw.Elapsed);
    }

    private static async Task<DbaTableCopyTableResult> CopyTableAsync(
        IDbaTableCopySource source,
        IDbaTableCopyDestination destination,
        DbaTableCopyDefinition definition,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var sourceRows = await source.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
        if (options.ClearDestination)
        {
            await destination.ClearAsync(definition, cancellationToken).ConfigureAwait(false);
        }

        if (sourceRows == 0)
        {
            long? emptyDestinationRows = null;
            var emptyVerified = true;
            if (options.VerifyRowCounts)
            {
                emptyDestinationRows = await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
                if (emptyDestinationRows.HasValue)
                {
                    emptyVerified = emptyDestinationRows.Value == 0;
                }
            }

            return new DbaTableCopyTableResult(definition.DisplayName, sourceRows, 0, emptyDestinationRows, emptyVerified);
        }

        long copied = 0;
        long offset = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await source.ReadPageAsync(
                    new DbaTableCopyPageRequest(definition, offset, options.PageSize),
                    cancellationToken)
                .ConfigureAwait(false);

            if (page.Rows.Count == 0)
            {
                break;
            }

            var destinationPage = DbaTableCopyPageTransformer.Transform(page, definition);
            await destination.WritePageAsync(definition, destinationPage, options, cancellationToken).ConfigureAwait(false);
            copied += destinationPage.Rows.Count;
            offset += page.Rows.Count;
            options.Progress?.Invoke(new DbaTableCopyProgress(definition.DisplayName, copied, sourceRows, destinationPage.Rows.Count));

            if (page.Rows.Count < options.PageSize)
            {
                break;
            }
        }

        long? destinationRows = null;
        var verified = true;
        if (options.VerifyRowCounts)
        {
            destinationRows = await destination.CountRowsAsync(definition, cancellationToken).ConfigureAwait(false);
            if (sourceRows.HasValue && destinationRows.HasValue)
            {
                verified = sourceRows.Value == destinationRows.Value;
            }
        }

        return new DbaTableCopyTableResult(definition.DisplayName, sourceRows, copied, destinationRows, verified);
    }

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
}
