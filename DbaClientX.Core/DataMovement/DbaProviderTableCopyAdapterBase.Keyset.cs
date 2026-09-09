using System.Data;

namespace DBAClientX.DataMovement;

public abstract partial class DbaProviderTableCopyAdapterBase
{
    private async Task<DbaTableCopyPage> ReadKeysetPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken)
    {
        request.Definition.Validate();
        if (Provider is not (DbaTableCopyProvider.SQLite or DbaTableCopyProvider.SqlServer))
            throw new NotSupportedException("Keyset table-copy reads currently support SQLite and SQL Server.");
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
        string query = Provider == DbaTableCopyProvider.SqlServer
            ? $"SELECT TOP ({request.PageSize}) * FROM {from}{where}{order}"
            : $"SELECT * FROM {from}{where}{order} LIMIT {request.PageSize}";
        DataTable table;
        try
        {
            table = await ExecuteBoundedPageCoreAsync(query, parameters, request.MaxBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (_treatMissingTablesAsEmpty && IsMissingTableException(exception))
        {
            table = new DataTable();
        }
        try
        {
            if (rankColumn != null && table.Columns.Contains(rankColumn)) table.Columns.Remove(rankColumn);
            table.TableName = request.Definition.DestinationName;
            string? nextToken = table.Rows.Count == 0 ? null : DbaKeysetContinuationToken.Encode(request.Definition, table.Rows[table.Rows.Count - 1]);
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

    private string BuildKeysetPredicate(IReadOnlyList<string> columns, object[]? values, IDictionary<string, object?> parameters)
    {
        if (values == null) return string.Empty;
        for (int index = 0; index < values.Length; index++) parameters.Add("@dbax_key" + index, values[index]);
        if (Provider == DbaTableCopyProvider.SQLite && columns.Count > 1)
            return $"({string.Join(", ", columns.Select(QuotePath))}) > ({string.Join(", ", parameters.Keys)})";
        var alternatives = new List<string>();
        for (int index = 0; index < columns.Count; index++)
        {
            var parts = new List<string>();
            for (int previous = 0; previous < index; previous++) parts.Add($"{QuotePath(columns[previous])} = @dbax_key{previous}");
            parts.Add($"{QuotePath(columns[index])} > @dbax_key{index}");
            alternatives.Add("(" + string.Join(" AND ", parts) + ")");
        }
        return string.Join(" OR ", alternatives);
    }

    /// <summary>Executes a parameterized keyset query with a row-payload limit. Providers opt in explicitly.</summary>
    protected virtual Task<DataTable> ExecuteBoundedPageCoreAsync(string query, IReadOnlyDictionary<string, object?> parameters, long? maxBytes, CancellationToken cancellationToken)
        => throw new NotSupportedException("This provider does not implement bounded keyset pages.");
}
