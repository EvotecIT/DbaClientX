using System.Data;
using Azure;
using DBAClientX.DataMovement;

namespace DBAClientX.AzureTables;

/// <summary>Connects Azure Table data to the provider-neutral DbaClientX copy engine.</summary>
public sealed class DbaAzureTablesAdapter :
    IDbaTableCopySource,
    IDbaTableCopyDestination,
    IDbaTableCopyPagePreflightDestination,
    IDbaTableCopyEmptyPageDestination,
    IDbaTableCopyMissingTableClassifier,
    IDbaTableCopyClearSafetyValidator
{
    private readonly IDbaAzureTableStore _store;
    private readonly DbaAzureTablesOptions _options;

    /// <summary>Creates an adapter over an Azure Tables store.</summary>
    public DbaAzureTablesAdapter(IDbaAzureTableStore store, DbaAzureTablesOptions? options = null)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _options = options ?? new DbaAzureTablesOptions();
        _options.Validate();
    }

    /// <summary>Creates an adapter from an Azure Storage or Cosmos DB Table API connection string.</summary>
    public DbaAzureTablesAdapter(string connectionString, DbaAzureTablesOptions? options = null)
        : this(new AzureSdkTableStore(connectionString), options)
    {
    }

    /// <summary>Normalized Azure Tables service endpoint when the SDK-backed store is used.</summary>
    public Uri? ServiceUri => (_store as AzureSdkTableStore)?.ServiceUri;

    async Task<long?> IDbaTableCopySource.CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
        => _options.EnableRowCounts
            ? await _store.CountAsync(definition.SourceName, _options.SourceFilter, cancellationToken).ConfigureAwait(false)
            : null;

    async Task<DbaTableCopyPage> IDbaTableCopySource.ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken)
    {
        var page = await _store.QueryPageAsync(
                request.Definition.SourceName,
                _options.SourceFilter,
                DbaAzureTableDataMapper.IncludeKeys(_options.SelectColumns),
                request.ContinuationToken,
                request.PageSize,
                cancellationToken)
            .ConfigureAwait(false);
        return new DbaTableCopyPage(
            DbaAzureTableDataMapper.ToDataTable(request.Definition.SourceName, page.Entities),
            page.ContinuationToken);
    }

    async Task IDbaTableCopyDestination.ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
    {
        if (!_options.AllowClearDestination)
        {
            throw new InvalidOperationException(
                "Clearing an Azure Table requires DbaAzureTablesOptions.AllowClearDestination = true.");
        }

        if (_options.CreateDestinationTable)
        {
            await _store.CreateTableIfNotExistsAsync(definition.DestinationName, cancellationToken).ConfigureAwait(false);
        }

        await _store.ClearAsync(definition.DestinationName, _options.BatchSize, cancellationToken).ConfigureAwait(false);
    }

    async Task IDbaTableCopyDestination.WritePageAsync(
        DbaTableCopyDefinition definition,
        DataTable page,
        DbaTableCopyOptions options,
        CancellationToken cancellationToken)
    {
        var batchSize = options.BatchSize ?? _options.BatchSize;
        if (batchSize is < 1 or > 100)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "Azure Table transaction size must be between 1 and 100.");
        }

        if (_options.CreateDestinationTable)
        {
            await _store.CreateTableIfNotExistsAsync(definition.DestinationName, cancellationToken).ConfigureAwait(false);
        }

        if (page.Rows.Count > 0)
        {
            await _store.WriteAsync(
                    definition.DestinationName,
                    DbaAzureTableDataMapper.ToEntities(page),
                    _options.WriteMode,
                    batchSize,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    async Task<long?> IDbaTableCopyDestination.CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
        => _options.EnableRowCounts
            ? await _store.CountAsync(definition.DestinationName, cancellationToken: cancellationToken).ConfigureAwait(false)
            : null;

    /// <inheritdoc />
    public void ValidatePage(DbaTableCopyDefinition definition, DataTable page)
    {
        if (page == null)
        {
            throw new ArgumentNullException(nameof(page));
        }
        _ = DbaAzureTableDataMapper.ToEntities(page);
    }

    /// <inheritdoc />
    public bool ShouldWriteEmptyPage(DbaTableCopyDefinition definition)
        => _options.CreateDestinationTable;

    /// <inheritdoc />
    public bool IsMissingTableException(Exception exception)
        => exception is RequestFailedException { Status: 404 };

    /// <inheritdoc />
    public void ValidateClearOperation(IDbaTableCopySource source, IReadOnlyList<DbaTableCopyDefinition> definitions)
    {
        if (source is not DbaAzureTablesAdapter sourceAdapter || !TargetsSameService(sourceAdapter))
        {
            return;
        }

        var comparer = ResolveTableNameComparer(sourceAdapter);
        foreach (var destinationDefinition in definitions)
        {
            foreach (var sourceDefinition in definitions)
            {
                if (comparer.Equals(destinationDefinition.DestinationName.Trim(), sourceDefinition.SourceName.Trim()))
                {
                    throw new InvalidOperationException(
                        $"Refusing to clear destination Azure Table '{destinationDefinition.DestinationName}' because it is also used as source table '{sourceDefinition.SourceName}' on the same service.");
                }
            }
        }
    }

    private bool TargetsSameService(DbaAzureTablesAdapter source)
    {
        if (ReferenceEquals(_store, source._store))
        {
            return true;
        }

        return ServiceUri != null && source.ServiceUri != null &&
               Uri.Compare(ServiceUri, source.ServiceUri, UriComponents.SchemeAndServer | UriComponents.Path, UriFormat.SafeUnescaped, StringComparison.OrdinalIgnoreCase) == 0;
    }

    private StringComparer ResolveTableNameComparer(DbaAzureTablesAdapter source)
    {
        DbaAzureTableNameComparison mode = _options.TableNameComparison != DbaAzureTableNameComparison.Auto
            ? _options.TableNameComparison
            : source._options.TableNameComparison;
        if (mode == DbaAzureTableNameComparison.Auto)
        {
            Uri? endpoint = ServiceUri ?? source.ServiceUri;
            mode = endpoint != null && endpoint.Host.IndexOf(".table.cosmos.", StringComparison.OrdinalIgnoreCase) >= 0
                ? DbaAzureTableNameComparison.Ordinal
                : DbaAzureTableNameComparison.OrdinalIgnoreCase;
        }

        return mode == DbaAzureTableNameComparison.Ordinal ? StringComparer.Ordinal : StringComparer.OrdinalIgnoreCase;
    }
}
