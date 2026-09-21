using System.Data;

namespace DBAClientX.DataMovement;

public abstract partial class DbaProviderTableCopyAdapterBase
{
    private async Task<DbaTableCopyPage> ReadKeysetPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken)
    {
        request.Definition.Validate();
        object[]? keys = DbaKeysetContinuationToken.Decode(request.Definition, request.ContinuationToken);
        var parameters = new Dictionary<string, object?>();
        string predicate = BuildKeysetPredicate(request.Definition.OrderByColumns!, keys, parameters);
        string from = QuotePath(request.Definition.SourceName);
        string? rankColumn = null;
        if (request.Definition.SourceOptions?.HasDeduplication == true)
        {
            DbaTableCopySourceOptions sourceOptions = request.Definition.SourceOptions;
            rankColumn = CreateDeduplicationRankColumn();
            string rank = QuoteSyntheticIdentifier(rankColumn);
            from = $"(SELECT {SourceAlias}.*, ROW_NUMBER() OVER (PARTITION BY {BuildDeduplicationKeyClause(sourceOptions, true)} ORDER BY {BuildDeduplicationWinnerOrderClause(sourceOptions)}) AS {rank} FROM {from} {SourceAlias}) dbax_deduped";
            predicate = $"{rank} = 1" + (predicate.Length == 0 ? "" : $" AND ({predicate})");
        }
        string where = predicate.Length == 0 ? "" : " WHERE " + predicate;
        string order = BuildOrderByClause(request.Definition.OrderByColumns);
        string query = Provider switch
        {
            DbaTableCopyProvider.SqlServer => $"SELECT TOP ({request.PageSize}) * FROM {from}{where}{order}",
            DbaTableCopyProvider.Oracle => $"SELECT * FROM {from}{where}{order} FETCH FIRST {request.PageSize} ROWS ONLY",
            _ => $"SELECT * FROM {from}{where}{order} LIMIT {request.PageSize}"
        };
        DataTable table;
        try
        {
            table = await ExecuteKeysetPageCoreAsync(request.Definition, query, parameters, request.MaxBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (_treatMissingTablesAsEmpty && IsMissingTableException(exception))
        {
            return new DbaTableCopyPage(new DataTable(request.Definition.DestinationName), null);
        }
        try
        {
            if (rankColumn != null && table.Columns.Contains(rankColumn)) table.Columns.Remove(rankColumn);
            IReadOnlyList<string> resultColumns = ResolveKeysetResultColumns(
                Provider,
                table.Columns,
                request.Definition.OrderByColumns!,
                request.Definition.SourceName);
            table.TableName = request.Definition.DestinationName;
            string? nextToken = table.Rows.Count == 0
                ? null
                : DbaKeysetContinuationToken.EncodeFromResultColumns(
                    request.Definition,
                    table.Rows[table.Rows.Count - 1],
                    resultColumns);
            if (nextToken != null && string.Equals(nextToken, request.ContinuationToken, StringComparison.Ordinal))
                throw new InvalidOperationException("The source key did not advance. Use a unique, non-null ascending key.");
            return new DbaTableCopyPage(table, nextToken);
        }
        catch
        {
            table.Dispose();
            throw;
        }
    }

    internal static IReadOnlyList<string> ResolveKeysetResultColumns(
        DbaTableCopyProvider provider,
        DataColumnCollection resultColumns,
        IReadOnlyList<string> orderByColumns,
        string sourceName)
    {
        var resolved = new string[orderByColumns.Count];
        for (var index = 0; index < orderByColumns.Count; index++)
        {
            string planned = orderByColumns[index];
            bool delimited = DbaIdentifierPath.IsDelimitedSegment(planned);
            string physical = DbaIdentifierPath.UnquoteSegment(planned, provider);
            bool emittedDelimited = delimited || DbaIdentifierPath.IsAutomaticallyDelimitedSegment(physical, provider);
            if (!emittedDelimited)
            {
                physical = provider switch
                {
                    DbaTableCopyProvider.PostgreSql => physical.ToLowerInvariant(),
                    DbaTableCopyProvider.Oracle => physical.ToUpperInvariant(),
                    _ => physical
                };
            }

            DataColumn? result = resultColumns.Cast<DataColumn>()
                .FirstOrDefault(column => string.Equals(column.ColumnName, physical, StringComparison.Ordinal));
            if (result == null && provider == DbaTableCopyProvider.SQLite)
            {
                string normalized = DbaIdentifierPath.NormalizeSqliteIdentifier(physical);
                result = resultColumns.Cast<DataColumn>().FirstOrDefault(column =>
                    DbaIdentifierPath.NormalizeSqliteIdentifier(column.ColumnName) == normalized);
            }
            if (result == null && provider is not DbaTableCopyProvider.PostgreSql and not DbaTableCopyProvider.Oracle &&
                resultColumns.Contains(physical))
            {
                result = resultColumns[physical];
            }
            if (result == null)
            {
                throw new InvalidOperationException(
                    $"Paging column '{planned}' does not exist in table '{sourceName}'.");
            }

            resolved[index] = result.ColumnName;
        }

        return resolved;
    }

    private string BuildKeysetPredicate(IReadOnlyList<string> columns, object[]? values, IDictionary<string, object?> parameters)
    {
        if (values == null) return string.Empty;
        for (int index = 0; index < values.Length; index++) parameters.Add(GetKeysetParameterName(index), values[index]);
        if (Provider is DbaTableCopyProvider.SQLite or DbaTableCopyProvider.PostgreSql or DbaTableCopyProvider.MySql && columns.Count > 1)
        {
            var parameterNames = Enumerable.Range(0, columns.Count).Select(GetKeysetParameterName);
            return $"({string.Join(", ", columns.Select(QuotePath))}) > ({string.Join(", ", parameterNames)})";
        }
        var alternatives = new List<string>();
        for (int index = 0; index < columns.Count; index++)
        {
            var parts = new List<string>();
            for (int previous = 0; previous < index; previous++) parts.Add($"{QuotePath(columns[previous])} = {GetKeysetParameterName(previous)}");
            parts.Add($"{QuotePath(columns[index])} > {GetKeysetParameterName(index)}");
            alternatives.Add("(" + string.Join(" AND ", parts) + ")");
        }
        return string.Join(" OR ", alternatives);
    }

    private string GetKeysetParameterName(int index)
        => (Provider == DbaTableCopyProvider.Oracle ? ":dbax_key" : "@dbax_key") + index;

    /// <summary>Executes a parameterized keyset query with a row-payload limit. Providers opt in explicitly.</summary>
    protected virtual Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => throw new NotSupportedException("This provider does not implement bounded keyset pages.");

    /// <summary>Executes a bounded keyset query with its source definition available for provider parameter typing.</summary>
    protected virtual Task<DataTable> ExecuteKeysetPageCoreAsync(DbaTableCopyDefinition definition, string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => ExecuteBoundedPageCoreAsync(query, parameters, maxBytes, cancellationToken);
}
