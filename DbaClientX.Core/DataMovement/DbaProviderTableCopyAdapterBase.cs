using System.Data;
using System.Data.Common;

namespace DBAClientX.DataMovement;

/// <summary>
/// Provides the reusable provider-backed paging, quoting, and count behavior for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public abstract class DbaProviderTableCopyAdapterBase : IDbaTableCopySource, IDbaTableCopyDestination, IDbaTableCopyPagePreflightDestination, IDbaTableCopyEmptyPageDestination
{
    private const string SourceAlias = "dbax_source";
    private const string DeduplicationRankColumnPrefix = "__DbaXR_";

    private readonly string[] _defaultOrderBy;
    private readonly bool _allowUnordered;
    private readonly bool _treatMissingTablesAsEmpty;

    /// <summary>
    /// Creates a provider-backed adapter base from provider settings.
    /// </summary>
    protected DbaProviderTableCopyAdapterBase(
        DbaTableCopyProvider provider,
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        bool treatMissingTablesAsEmpty = false)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("ConnectionString cannot be null or whitespace.", nameof(connectionString));
        }

        Provider = provider;
        ConnectionString = connectionString;
        _defaultOrderBy = defaultOrderByColumns?
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .Select(static value => value.Trim())
            .ToArray()
            ?? Array.Empty<string>();
        _allowUnordered = allowUnordered;
        _treatMissingTablesAsEmpty = treatMissingTablesAsEmpty;
    }

    /// <summary>Provider implemented by this adapter.</summary>
    protected DbaTableCopyProvider Provider { get; }

    /// <summary>Connection string used by this adapter.</summary>
    protected string ConnectionString { get; }

    /// <inheritdoc />
    public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteCountAsync(definition.SourceName, definition.SourceOptions, treatMissingAsEmpty: true, cancellationToken);

    /// <inheritdoc />
    public async Task<DataTable> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
    {
        var query = BuildPageQuery(
            request.Definition.SourceName,
            request.Definition.OrderByColumns,
            request.Definition.SourceOptions,
            request.Offset,
            request.PageSize);
        DataTable table;
        try
        {
            table = await ExecuteTableAsync(query, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (_treatMissingTablesAsEmpty && IsMissingTableException(ex))
        {
            table = new DataTable(request.Definition.DestinationName);
        }

        if (request.Definition.SourceOptions?.HasDeduplication == true &&
            table.Columns.Count > 0 &&
            IsDeduplicationRankColumn(table.Columns[table.Columns.Count - 1].ColumnName))
        {
            table.Columns.RemoveAt(table.Columns.Count - 1);
        }

        table.TableName = request.Definition.DestinationName;
        return table;
    }

    /// <inheritdoc />
    public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteNonQueryIgnoreMissingAsync($"DELETE FROM {QuotePath(definition.DestinationName)}", cancellationToken);

    /// <inheritdoc />
    public abstract Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default);

    Task<long?> IDbaTableCopyDestination.CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
        => ExecuteCountAsync(definition.DestinationName, null, treatMissingAsEmpty: false, cancellationToken);

    /// <inheritdoc />
    public virtual void ValidatePage(DbaTableCopyDefinition definition, DataTable page)
    {
    }

    /// <inheritdoc />
    public virtual bool ShouldWriteEmptyPage(DbaTableCopyDefinition definition)
        => false;

    private async Task<long?> ExecuteCountAsync(
        string tableName,
        DbaTableCopySourceOptions? sourceOptions,
        bool treatMissingAsEmpty,
        CancellationToken cancellationToken)
    {
        object? result;
        try
        {
            result = await ExecuteScalarAsync(BuildCountQuery(tableName, sourceOptions), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (_treatMissingTablesAsEmpty && IsMissingTableException(ex))
        {
            return treatMissingAsEmpty ? 0 : null;
        }

        return result == null || result == DBNull.Value
            ? null
            : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    private Task<object?> ExecuteScalarAsync(string query, CancellationToken cancellationToken)
        => ExecuteScalarCoreAsync(query, cancellationToken);

    private Task<DataTable> ExecuteTableAsync(string query, CancellationToken cancellationToken)
        => ExecuteTableCoreAsync(query, cancellationToken);

    private Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken)
        => ExecuteNonQueryCoreAsync(query, cancellationToken);

    /// <summary>Executes a scalar SQL statement using the concrete provider.</summary>
    protected abstract Task<object?> ExecuteScalarCoreAsync(string query, CancellationToken cancellationToken);

    /// <summary>Executes a table-returning SQL statement using the concrete provider.</summary>
    protected abstract Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken);

    /// <summary>Executes a non-query SQL statement using the concrete provider.</summary>
    protected abstract Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken);

    private async Task ExecuteNonQueryIgnoreMissingAsync(string query, CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteNonQueryAsync(query, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (_treatMissingTablesAsEmpty && IsMissingTableException(ex))
        {
            // Missing tables are intentionally ignored when the caller opted into schema-version tolerant copies.
            return;
        }
    }

    private string BuildPageQuery(
        string tableName,
        IReadOnlyList<string>? orderByColumns,
        DbaTableCopySourceOptions? sourceOptions,
        long offset,
        int pageSize)
    {
        if (sourceOptions?.HasDeduplication == true)
        {
            return BuildDeduplicatedPageQuery(tableName, orderByColumns, sourceOptions, offset, pageSize);
        }

        var quotedTable = QuotePath(tableName);
        var orderBy = BuildOrderByClause(orderByColumns);
        return Provider switch
        {
            DbaTableCopyProvider.SqlServer or DbaTableCopyProvider.Oracle
                => $"SELECT * FROM {quotedTable}{orderBy} OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY",
            _ => $"SELECT * FROM {quotedTable}{orderBy} LIMIT {pageSize} OFFSET {offset}"
        };
    }

    private string BuildCountQuery(string tableName, DbaTableCopySourceOptions? sourceOptions)
    {
        var quotedTable = QuotePath(tableName);
        if (sourceOptions?.HasDeduplication != true)
        {
            return $"SELECT COUNT(*) FROM {quotedTable}";
        }

        var keyColumns = BuildDeduplicationKeyClause(sourceOptions, withSourceAlias: false);
        return $"SELECT COUNT(*) FROM (SELECT 1 AS dbax_key FROM {quotedTable} GROUP BY {keyColumns}) dbax_source_keys";
    }

    private string BuildDeduplicatedPageQuery(
        string tableName,
        IReadOnlyList<string>? orderByColumns,
        DbaTableCopySourceOptions sourceOptions,
        long offset,
        int pageSize)
    {
        var quotedTable = QuotePath(tableName);
        var keyColumns = BuildDeduplicationKeyClause(sourceOptions, withSourceAlias: true);
        var winnerOrder = BuildDeduplicationWinnerOrderClause(sourceOptions);
        var outerOrder = BuildOrderByClause(orderByColumns ?? sourceOptions.DeduplicateByColumns);
        var rankColumn = CreateDeduplicationRankColumn();
        var rank = QuoteSyntheticIdentifier(rankColumn);
        var rankedSource =
            $"SELECT {SourceAlias}.*, ROW_NUMBER() OVER (PARTITION BY {keyColumns} ORDER BY {winnerOrder}) AS {rank} " +
            $"FROM {quotedTable} {SourceAlias}";

        return Provider switch
        {
            DbaTableCopyProvider.SqlServer or DbaTableCopyProvider.Oracle
                => $"SELECT * FROM ({rankedSource}) dbax_deduped WHERE {rank} = 1{outerOrder} OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY",
            _ => $"SELECT * FROM ({rankedSource}) dbax_deduped WHERE {rank} = 1{outerOrder} LIMIT {pageSize} OFFSET {offset}"
        };
    }

    private string BuildDeduplicationKeyClause(DbaTableCopySourceOptions sourceOptions, bool withSourceAlias)
        => string.Join(
            ", ",
            sourceOptions.DeduplicateByColumns!
                .Where(static column => !string.IsNullOrWhiteSpace(column))
                .Select(column => BuildDeduplicationKeyColumn(column.Trim(), sourceOptions.DeduplicateCaseInsensitive, withSourceAlias)));

    private string BuildDeduplicationKeyColumn(string columnName, bool caseInsensitive, bool withSourceAlias)
    {
        var quoted = withSourceAlias
            ? QuotePath(SourceAlias + "." + columnName)
            : QuotePath(columnName);
        return caseInsensitive ? $"LOWER({quoted})" : quoted;
    }

    private string BuildDeduplicationWinnerOrderClause(DbaTableCopySourceOptions sourceOptions)
    {
        var orderColumns = sourceOptions.DeduplicateOrderByColumns is { Count: > 0 }
            ? sourceOptions.DeduplicateOrderByColumns
            : sourceOptions.DeduplicateByColumns!;
        var clauses = new List<string>();
        clauses.AddRange(orderColumns
            .Where(static column => !string.IsNullOrWhiteSpace(column))
            .Select(column => QuotePath(SourceAlias + "." + column.Trim()) + " DESC"));
        clauses.AddRange(sourceOptions.DeduplicateByColumns!
            .Where(static column => !string.IsNullOrWhiteSpace(column))
            .Select(column => QuotePath(SourceAlias + "." + column.Trim())));
        return string.Join(", ", clauses);
    }

    private string BuildOrderByClause(IReadOnlyList<string>? orderByColumns)
    {
        var effectiveOrderBy = orderByColumns is { Count: > 0 }
            ? orderByColumns.Where(static value => !string.IsNullOrWhiteSpace(value)).Select(static value => value.Trim()).ToArray()
            : _defaultOrderBy;
        if (effectiveOrderBy.Length > 0)
        {
            return " ORDER BY " + string.Join(", ", effectiveOrderBy.Select(QuotePath));
        }

        if (!_allowUnordered)
        {
            throw new InvalidOperationException("OrderBy is required for deterministic paged table copy. Set AllowUnordered for ad hoc copies where natural provider order is acceptable.");
        }

        return Provider switch
        {
            DbaTableCopyProvider.SqlServer => " ORDER BY (SELECT NULL)",
            _ => string.Empty
        };
    }

    /// <summary>
    /// Quotes a provider identifier path such as schema and table segments.
    /// </summary>
    protected string QuotePath(string identifierPath)
        => string.Join(".", SplitIdentifierPath(identifierPath).Select(QuoteIdentifier));

    private IReadOnlyList<IdentifierSegment> SplitIdentifierPath(string identifierPath)
    {
        if (string.IsNullOrWhiteSpace(identifierPath))
        {
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifierPath));
        }

        var parts = SplitIdentifierPathSegments(identifierPath);
        if (parts.Any(static part => string.IsNullOrWhiteSpace(part)))
        {
            throw new ArgumentException($"Identifier '{identifierPath}' contains an empty path segment.", nameof(identifierPath));
        }

        return parts.Select(NormalizeIdentifierSegment).ToArray();
    }

    private static IReadOnlyList<string> SplitIdentifierPathSegments(string identifierPath)
    {
        var parts = new List<string>();
        var start = 0;
        var quote = '\0';
        for (var index = 0; index < identifierPath.Length; index++)
        {
            var value = identifierPath[index];
            if (quote == '\0')
            {
                if (value is '"' or '[' or '`')
                {
                    quote = value;
                    continue;
                }

                if (value == '.')
                {
                    parts.Add(identifierPath.Substring(start, index - start));
                    start = index + 1;
                }

                continue;
            }

            if (quote == '"' && value == '"')
            {
                if (index + 1 < identifierPath.Length && identifierPath[index + 1] == '"')
                {
                    index++;
                    continue;
                }

                quote = '\0';
                continue;
            }

            if (quote == '[' && value == ']')
            {
                if (index + 1 < identifierPath.Length && identifierPath[index + 1] == ']')
                {
                    index++;
                    continue;
                }

                quote = '\0';
                continue;
            }

            if (quote == '`' && value == '`')
            {
                if (index + 1 < identifierPath.Length && identifierPath[index + 1] == '`')
                {
                    index++;
                    continue;
                }

                quote = '\0';
            }
        }

        if (quote != '\0')
        {
            throw new ArgumentException($"Identifier '{identifierPath}' contains an unterminated delimited path segment.", nameof(identifierPath));
        }

        parts.Add(identifierPath.Substring(start));
        return parts;
    }

    private IdentifierSegment NormalizeIdentifierSegment(string identifier)
    {
        var trimmed = identifier.Trim();
        if ((Provider == DbaTableCopyProvider.SqlServer || Provider == DbaTableCopyProvider.SQLite) &&
            trimmed.Length >= 2 &&
            trimmed[0] == '[' &&
            trimmed[trimmed.Length - 1] == ']')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]"), Provider == DbaTableCopyProvider.SQLite);
        }

        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[trimmed.Length - 1] == '"')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\""), true);
        }

        if ((Provider == DbaTableCopyProvider.MySql || Provider == DbaTableCopyProvider.SQLite) &&
            trimmed.Length >= 2 &&
            trimmed[0] == '`' &&
            trimmed[trimmed.Length - 1] == '`')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("``", "`"), Provider == DbaTableCopyProvider.SQLite);
        }

        return new IdentifierSegment(trimmed, false);
    }

    /// <summary>
    /// Returns a provider-quoted bulk destination table name.
    /// </summary>
    protected string NormalizeQuotedBulkDestinationTableName(string destinationTableName)
        => QuotePath(destinationTableName);

    /// <summary>
    /// Returns a SQLite bulk destination name while preserving explicitly quoted dotted identifiers.
    /// </summary>
    protected string NormalizeSQLiteBulkDestinationTableName(string destinationTableName)
    {
        return string.Join(
            ".",
            SplitIdentifierPath(destinationTableName).Select(static segment =>
                segment.IsExplicitlyQuoted
                    ? "\"" + segment.Value.Replace("\"", "\"\"") + "\""
                    : segment.Value));
    }

    /// <summary>
    /// Returns a MySQL connection string suitable for regular reads by removing bulk-only local-infile options.
    /// </summary>
    protected string ResolveMySqlRegularOperationConnectionString()
    {
        var builder = new DbConnectionStringBuilder { ConnectionString = ConnectionString };
        RemoveConnectionStringOption(builder, "AllowLoadLocalInfile");
        RemoveConnectionStringOption(builder, "Allow Load Local Infile");
        return builder.ConnectionString;
    }

    private static void RemoveConnectionStringOption(DbConnectionStringBuilder builder, string optionName)
    {
        var matchingKey = builder.Keys
            .Cast<string>()
            .FirstOrDefault(key => string.Equals(key, optionName, StringComparison.OrdinalIgnoreCase));
        if (matchingKey != null)
        {
            builder.Remove(matchingKey);
        }
    }

    private string QuoteIdentifier(IdentifierSegment segment)
    {
        if (segment.IsExplicitlyQuoted &&
            (Provider == DbaTableCopyProvider.PostgreSql || Provider == DbaTableCopyProvider.Oracle))
        {
            return "\"" + segment.Value.Replace("\"", "\"\"") + "\"";
        }

        return QuoteIdentifier(segment.Value);
    }

    private string QuoteIdentifier(string identifier)
    {
        if (identifier.IndexOfAny(new[] { ';', '\r', '\n', '\0' }) >= 0)
        {
            throw new ArgumentException($"Identifier '{identifier}' contains unsupported characters.", nameof(identifier));
        }

        return Provider switch
        {
            DbaTableCopyProvider.SqlServer => "[" + identifier.Replace("]", "]]") + "]",
            DbaTableCopyProvider.MySql => "`" + identifier.Replace("`", "``") + "`",
            DbaTableCopyProvider.Oracle => QuoteOracleIdentifier(identifier),
            DbaTableCopyProvider.PostgreSql => QuotePostgreSqlIdentifier(identifier),
            _ => "\"" + identifier.Replace("\"", "\"\"") + "\""
        };
    }

    private string QuoteSyntheticIdentifier(string identifier)
    {
        if (identifier.IndexOfAny(new[] { ';', '\r', '\n', '\0' }) >= 0)
        {
            throw new ArgumentException($"Identifier '{identifier}' contains unsupported characters.", nameof(identifier));
        }

        return Provider switch
        {
            DbaTableCopyProvider.SqlServer => "[" + identifier.Replace("]", "]]") + "]",
            DbaTableCopyProvider.MySql => "`" + identifier.Replace("`", "``") + "`",
            _ => "\"" + identifier.Replace("\"", "\"\"") + "\""
        };
    }

    private static string QuoteOracleIdentifier(string identifier)
        => IsOracleSimpleIdentifier(identifier) &&
           !DbaIdentifierPath.IsReservedIdentifier(identifier, DbaTableCopyProvider.Oracle)
            ? identifier.ToUpperInvariant()
            : "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static bool IsOracleSimpleIdentifier(string identifier)
    {
        if (identifier.Length == 0 || !IsOracleIdentifierStart(identifier[0]))
        {
            return false;
        }

        for (var i = 1; i < identifier.Length; i++)
        {
            if (!IsOracleIdentifierPart(identifier[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsOracleIdentifierStart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z';

    private static bool IsOracleIdentifierPart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_' or >= '0' and <= '9' or '$' or '#';

    private static string QuotePostgreSqlIdentifier(string identifier)
        => IsPostgreSqlSimpleIdentifier(identifier) &&
           !DbaIdentifierPath.IsReservedIdentifier(identifier, DbaTableCopyProvider.PostgreSql)
            ? identifier.ToLowerInvariant()
            : "\"" + identifier.Replace("\"", "\"\"") + "\"";

    private static bool IsPostgreSqlSimpleIdentifier(string identifier)
    {
        if (identifier.Length == 0 || !IsPostgreSqlIdentifierStart(identifier[0]))
        {
            return false;
        }

        for (var i = 1; i < identifier.Length; i++)
        {
            if (!IsPostgreSqlIdentifierPart(identifier[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsPostgreSqlIdentifierStart(char value)
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_';

    private static bool IsPostgreSqlIdentifierPart(char value)
        => IsPostgreSqlIdentifierStart(value) || value is >= '0' and <= '9' or '$';

    private readonly struct IdentifierSegment
    {
        public IdentifierSegment(string value, bool isExplicitlyQuoted)
        {
            Value = value;
            IsExplicitlyQuoted = isExplicitlyQuoted;
        }

        public string Value { get; }

        public bool IsExplicitlyQuoted { get; }
    }

    private static bool IsMissingTableException(Exception exception)
    {
        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            var message = current.Message;
            if (message.Contains("no such column", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("unknown column", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("invalid column name", StringComparison.OrdinalIgnoreCase) ||
                (message.Contains("column", StringComparison.OrdinalIgnoreCase) &&
                 message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)))
            {
                return false;
            }

            if (message.Contains("no such table", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("invalid object name", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("relation", StringComparison.OrdinalIgnoreCase) && message.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("schema", StringComparison.OrdinalIgnoreCase) && message.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("table", StringComparison.OrdinalIgnoreCase) && message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("table", StringComparison.OrdinalIgnoreCase) && message.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("view", StringComparison.OrdinalIgnoreCase) && message.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("table or view does not exist", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsDeduplicationRankColumn(string columnName)
        => columnName.StartsWith(DeduplicationRankColumnPrefix, StringComparison.Ordinal);

    private static string CreateDeduplicationRankColumn()
        => DeduplicationRankColumnPrefix + Guid.NewGuid().ToString("N").Substring(0, 16);

}
