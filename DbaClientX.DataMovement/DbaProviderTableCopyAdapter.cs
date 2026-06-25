using Microsoft.Data.Sqlite;
using DBAClientX;

namespace DBAClientX.DataMovement;

/// <summary>
/// Provides reusable provider-backed source and destination adapters for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed class DbaProviderTableCopyAdapter : IDbaTableCopySource, IDbaTableCopyDestination
{
    private const string SourceAlias = "dbax_source";
    private const string DeduplicationRankColumn = "__DbaXCopyRank_62D977CD8E7A4BC08D1A73B5197F33D4";

    private readonly DbaTableCopyProvider _provider;
    private readonly string _connectionString;
    private readonly string[] _defaultOrderBy;
    private readonly bool _allowUnordered;
    private readonly SqlServerBulkInsertOptions? _sqlServerOptions;
    private readonly bool _treatMissingTablesAsEmpty;

    /// <summary>
    /// Creates a provider-backed adapter from explicit provider settings.
    /// </summary>
    public DbaProviderTableCopyAdapter(
        DbaTableCopyProvider provider,
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        SqlServerBulkInsertOptions? sqlServerOptions = null,
        bool treatMissingTablesAsEmpty = false)
        : this(new DbaProviderTableCopyAdapterOptions
        {
            Provider = provider,
            ConnectionString = connectionString,
            DefaultOrderByColumns = defaultOrderByColumns,
            AllowUnordered = allowUnordered,
            SqlServerOptions = sqlServerOptions,
            TreatMissingTablesAsEmpty = treatMissingTablesAsEmpty
        })
    {
    }

    /// <summary>
    /// Creates a provider-backed adapter from options.
    /// </summary>
    public DbaProviderTableCopyAdapter(DbaProviderTableCopyAdapterOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new ArgumentException("ConnectionString cannot be null or whitespace.", nameof(options));
        }

        _provider = options.Provider;
        _connectionString = options.ConnectionString;
        _defaultOrderBy = options.DefaultOrderByColumns?
            .Where(static value => !string.IsNullOrWhiteSpace(value))
            .Select(static value => value.Trim())
            .ToArray()
            ?? Array.Empty<string>();
        _allowUnordered = options.AllowUnordered;
        _sqlServerOptions = options.SqlServerOptions;
        _treatMissingTablesAsEmpty = options.TreatMissingTablesAsEmpty;
    }

    /// <inheritdoc />
    public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteCountAsync(definition.SourceName, definition.SourceOptions, cancellationToken);

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

        if (table.Columns.Contains(DeduplicationRankColumn))
        {
            table.Columns.Remove(DeduplicationRankColumn);
        }

        table.TableName = request.Definition.DestinationName;
        return table;
    }

    /// <inheritdoc />
    public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteNonQueryIgnoreMissingAsync($"DELETE FROM {QuotePath(definition.DestinationName)}", cancellationToken);

    /// <inheritdoc />
    public async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        switch (_provider)
        {
            case DbaTableCopyProvider.SqlServer:
                using (var sqlServer = new SqlServer())
                {
                    await sqlServer.BulkInsertAsync(
                            _connectionString,
                            page,
                            definition.DestinationName,
                            _sqlServerOptions,
                            batchSize: options.BatchSize,
                            bulkCopyTimeout: options.BulkCopyTimeout,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.PostgreSql:
                using (var postgreSql = new PostgreSql())
                {
                    var bulkPage = NormalizePostgreSqlBulkPage(page);
                    await postgreSql.BulkInsertAsync(
                            _connectionString,
                            bulkPage,
                            NormalizePostgreSqlBulkDestinationTableName(definition.DestinationName),
                            batchSize: options.BatchSize,
                            bulkCopyTimeout: options.BulkCopyTimeout,
                            cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.MySql:
                using (var mySql = new MySql())
                {
                    await mySql.BulkInsertAsync(_connectionString, page, definition.DestinationName, batchSize: options.BatchSize, bulkCopyTimeout: options.BulkCopyTimeout, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.Oracle:
                using (var oracle = new Oracle())
                {
                    await oracle.BulkInsertAsync(_connectionString, page, definition.DestinationName, batchSize: options.BatchSize, bulkCopyTimeout: options.BulkCopyTimeout, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.SQLite:
                using (var sqlite = new SQLite())
                {
                    await sqlite.BulkInsertWithConnectionStringAsync(ResolveSQLiteConnectionString(), page, definition.DestinationName, batchSize: options.BatchSize, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            default:
                throw new NotSupportedException($"Provider '{_provider}' is not supported.");
        }
    }

    Task<long?> IDbaTableCopyDestination.CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken)
        => ExecuteCountAsync(definition.DestinationName, null, cancellationToken);

    private async Task<long?> ExecuteCountAsync(string tableName, DbaTableCopySourceOptions? sourceOptions, CancellationToken cancellationToken)
    {
        object? result;
        try
        {
            result = await ExecuteScalarAsync(BuildCountQuery(tableName, sourceOptions), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (_treatMissingTablesAsEmpty && IsMissingTableException(ex))
        {
            return 0;
        }

        return result == null || result == DBNull.Value
            ? null
            : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    private async Task<object?> ExecuteScalarAsync(string query, CancellationToken cancellationToken)
    {
        switch (_provider)
        {
            case DbaTableCopyProvider.SqlServer:
                using (var sqlServer = new SqlServer())
                {
                    return await sqlServer.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaTableCopyProvider.PostgreSql:
                using (var postgreSql = new PostgreSql())
                {
                    return await postgreSql.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaTableCopyProvider.MySql:
                using (var mySql = new MySql())
                {
                    return await mySql.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaTableCopyProvider.Oracle:
                using (var oracle = new Oracle())
                {
                    return await oracle.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaTableCopyProvider.SQLite:
                return await ExecuteSQLiteScalarAsync(query, cancellationToken).ConfigureAwait(false);
            default:
                throw new NotSupportedException($"Provider '{_provider}' is not supported.");
        }
    }

    private async Task<DataTable> ExecuteTableAsync(string query, CancellationToken cancellationToken)
    {
        object? result;
        switch (_provider)
        {
            case DbaTableCopyProvider.SqlServer:
                using (var sqlServer = new SqlServer { ReturnType = ReturnType.DataTable })
                {
                    result = await sqlServer.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.PostgreSql:
                using (var postgreSql = new PostgreSql { ReturnType = ReturnType.DataTable })
                {
                    result = await postgreSql.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.MySql:
                using (var mySql = new MySql { ReturnType = ReturnType.DataTable })
                {
                    result = await mySql.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.Oracle:
                using (var oracle = new Oracle { ReturnType = ReturnType.DataTable })
                {
                    result = await oracle.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.SQLite:
                return await ExecuteSQLiteTableAsync(query, cancellationToken).ConfigureAwait(false);
            default:
                throw new NotSupportedException($"Provider '{_provider}' is not supported.");
        }

        return result as DataTable
            ?? throw new InvalidOperationException($"Provider '{_provider}' did not return a DataTable.");
    }

    private async Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken)
    {
        switch (_provider)
        {
            case DbaTableCopyProvider.SqlServer:
                using (var sqlServer = new SqlServer())
                {
                    await sqlServer.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.PostgreSql:
                using (var postgreSql = new PostgreSql())
                {
                    await postgreSql.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.MySql:
                using (var mySql = new MySql())
                {
                    await mySql.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.Oracle:
                using (var oracle = new Oracle())
                {
                    await oracle.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaTableCopyProvider.SQLite:
                await ExecuteSQLiteNonQueryAsync(query, cancellationToken).ConfigureAwait(false);
                break;
            default:
                throw new NotSupportedException($"Provider '{_provider}' is not supported.");
        }
    }

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
        return _provider switch
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
        return $"SELECT COUNT(*) FROM (SELECT 1 FROM {quotedTable} GROUP BY {keyColumns}) dbax_source_keys";
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
        var rank = QuoteSyntheticIdentifier(DeduplicationRankColumn);
        var rankedSource =
            $"SELECT {SourceAlias}.*, ROW_NUMBER() OVER (PARTITION BY {keyColumns} ORDER BY {winnerOrder}) AS {rank} " +
            $"FROM {quotedTable} {SourceAlias}";

        return _provider switch
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

        return _provider switch
        {
            DbaTableCopyProvider.SqlServer => " ORDER BY (SELECT NULL)",
            DbaTableCopyProvider.Oracle => " ORDER BY 1",
            _ => string.Empty
        };
    }

    private string QuotePath(string identifierPath)
        => string.Join(".", SplitIdentifierPath(identifierPath).Select(QuoteIdentifier));

    private IReadOnlyList<IdentifierSegment> SplitIdentifierPath(string identifierPath)
    {
        if (string.IsNullOrWhiteSpace(identifierPath))
        {
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifierPath));
        }

        var parts = identifierPath.Split('.');
        if (parts.Any(static part => string.IsNullOrWhiteSpace(part)))
        {
            throw new ArgumentException($"Identifier '{identifierPath}' contains an empty path segment.", nameof(identifierPath));
        }

        return parts.Select(NormalizeIdentifierSegment).ToArray();
    }

    private IdentifierSegment NormalizeIdentifierSegment(string identifier)
    {
        var trimmed = identifier.Trim();
        if (_provider == DbaTableCopyProvider.SqlServer &&
            trimmed.Length >= 2 &&
            trimmed[0] == '[' &&
            trimmed[trimmed.Length - 1] == ']')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]"), false);
        }

        if (trimmed.Length >= 2 &&
            trimmed[0] == '"' &&
            trimmed[trimmed.Length - 1] == '"')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\""), true);
        }

        if (_provider == DbaTableCopyProvider.MySql &&
            trimmed.Length >= 2 &&
            trimmed[0] == '`' &&
            trimmed[trimmed.Length - 1] == '`')
        {
            return new IdentifierSegment(trimmed.Substring(1, trimmed.Length - 2).Replace("``", "`"), false);
        }

        return new IdentifierSegment(trimmed, false);
    }

    private string NormalizePostgreSqlBulkDestinationTableName(string destinationTableName)
    {
        if (_provider != DbaTableCopyProvider.PostgreSql)
        {
            return destinationTableName;
        }

        return string.Join(
            ".",
            SplitIdentifierPath(destinationTableName).Select(static segment =>
                !segment.IsExplicitlyQuoted && IsPostgreSqlSimpleIdentifier(segment.Value)
                    ? segment.Value.ToLowerInvariant()
                    : segment.Value));
    }

    private DataTable NormalizePostgreSqlBulkPage(DataTable page)
    {
        if (_provider != DbaTableCopyProvider.PostgreSql)
        {
            return page;
        }

        var normalizedNames = page.Columns
            .Cast<DataColumn>()
            .Select(static column => NormalizePostgreSqlBulkColumnName(column.ColumnName))
            .ToArray();
        if (page.Columns.Cast<DataColumn>().Select(static column => column.ColumnName).SequenceEqual(normalizedNames, StringComparer.Ordinal))
        {
            return page;
        }

        var duplicates = normalizedNames
            .GroupBy(static name => name, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault(static group => group.Count() > 1);
        if (duplicates != null)
        {
            throw new InvalidOperationException($"PostgreSQL bulk copy column normalization would create duplicate destination column '{duplicates.Key}'.");
        }

        var normalized = page.Copy();
        for (var i = 0; i < normalized.Columns.Count; i++)
        {
            normalized.Columns[i].ColumnName = normalizedNames[i];
        }

        return normalized;
    }

    private static string NormalizePostgreSqlBulkColumnName(string columnName)
    {
        var trimmed = columnName.Trim();
        if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[trimmed.Length - 1] == '"')
        {
            return trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\"");
        }

        return IsPostgreSqlSimpleIdentifier(trimmed)
            ? trimmed.ToLowerInvariant()
            : trimmed;
    }

    private string QuoteIdentifier(IdentifierSegment segment)
    {
        if (segment.IsExplicitlyQuoted &&
            (_provider == DbaTableCopyProvider.PostgreSql || _provider == DbaTableCopyProvider.Oracle))
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

        return _provider switch
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

        return _provider switch
        {
            DbaTableCopyProvider.SqlServer => "[" + identifier.Replace("]", "]]") + "]",
            DbaTableCopyProvider.MySql => "`" + identifier.Replace("`", "``") + "`",
            _ => "\"" + identifier.Replace("\"", "\"\"") + "\""
        };
    }

    private static string QuoteOracleIdentifier(string identifier)
        => IsOracleSimpleIdentifier(identifier)
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
        => IsPostgreSqlSimpleIdentifier(identifier)
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
            if (message.Contains("no such table", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("invalid object name", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("does not exist", StringComparison.OrdinalIgnoreCase) ||
                message.Contains("table or view does not exist", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private string ResolveSQLiteConnectionString()
    {
        if (!HasSQLiteConnectionString())
        {
            return SQLite.BuildConnectionString(_connectionString);
        }

        var builder = new SqliteConnectionStringBuilder(_connectionString)
        {
            Pooling = false
        };

        return builder.ConnectionString;
    }

    private bool HasSQLiteConnectionString()
        => _connectionString.Contains("=", StringComparison.Ordinal);

    private async Task<object?> ExecuteSQLiteScalarAsync(string query, CancellationToken cancellationToken)
    {
        using (var connection = new SqliteConnection(ResolveSQLiteConnectionString()))
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<DataTable> ExecuteSQLiteTableAsync(string query, CancellationToken cancellationToken)
    {
        using (var connection = new SqliteConnection(ResolveSQLiteConnectionString()))
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    var table = new DataTable();
                    table.Load(reader);
                    return table;
                }
            }
        }
    }

    private async Task ExecuteSQLiteNonQueryAsync(string query, CancellationToken cancellationToken)
    {
        using (var connection = new SqliteConnection(ResolveSQLiteConnectionString()))
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
