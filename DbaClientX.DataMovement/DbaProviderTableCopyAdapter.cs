using Microsoft.Data.Sqlite;
using DBAClientX;

namespace DBAClientX.DataMovement;

/// <summary>
/// Provides reusable provider-backed source and destination adapters for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed class DbaProviderTableCopyAdapter : IDbaTableCopySource, IDbaTableCopyDestination
{
    private readonly DbaTableCopyProvider _provider;
    private readonly string _connectionString;
    private readonly string[] _defaultOrderBy;
    private readonly bool _allowUnordered;
    private readonly SqlServerBulkInsertOptions? _sqlServerOptions;

    /// <summary>
    /// Creates a provider-backed adapter from explicit provider settings.
    /// </summary>
    public DbaProviderTableCopyAdapter(
        DbaTableCopyProvider provider,
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        SqlServerBulkInsertOptions? sqlServerOptions = null)
        : this(new DbaProviderTableCopyAdapterOptions
        {
            Provider = provider,
            ConnectionString = connectionString,
            DefaultOrderByColumns = defaultOrderByColumns,
            AllowUnordered = allowUnordered,
            SqlServerOptions = sqlServerOptions
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
    }

    /// <inheritdoc />
    public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteCountAsync(definition.SourceName, cancellationToken);

    /// <inheritdoc />
    public async Task<DataTable> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
    {
        var query = BuildPageQuery(request.Definition.SourceName, request.Definition.OrderByColumns, request.Offset, request.PageSize);
        var table = await ExecuteTableAsync(query, cancellationToken).ConfigureAwait(false);
        table.TableName = request.Definition.DestinationName;
        return table;
    }

    /// <inheritdoc />
    public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteNonQueryAsync($"DELETE FROM {QuotePath(definition.DestinationName)}", cancellationToken);

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
                    await postgreSql.BulkInsertAsync(_connectionString, page, definition.DestinationName, batchSize: options.BatchSize, bulkCopyTimeout: options.BulkCopyTimeout, cancellationToken: cancellationToken).ConfigureAwait(false);
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
        => ExecuteCountAsync(definition.DestinationName, cancellationToken);

    private async Task<long?> ExecuteCountAsync(string tableName, CancellationToken cancellationToken)
    {
        var result = await ExecuteScalarAsync($"SELECT COUNT(*) FROM {QuotePath(tableName)}", cancellationToken).ConfigureAwait(false);
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

    private string BuildPageQuery(string tableName, IReadOnlyList<string>? orderByColumns, long offset, int pageSize)
    {
        var quotedTable = QuotePath(tableName);
        var orderBy = BuildOrderByClause(orderByColumns);
        return _provider switch
        {
            DbaTableCopyProvider.SqlServer or DbaTableCopyProvider.Oracle
                => $"SELECT * FROM {quotedTable}{orderBy} OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY",
            _ => $"SELECT * FROM {quotedTable}{orderBy} LIMIT {pageSize} OFFSET {offset}"
        };
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

    private static IReadOnlyList<string> SplitIdentifierPath(string identifierPath)
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

        return parts.Select(static part => part.Trim()).ToArray();
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
        => value is >= 'A' and <= 'Z' or >= 'a' and <= 'z' or '_';

    private static bool IsOracleIdentifierPart(char value)
        => IsOracleIdentifierStart(value) || value is >= '0' and <= '9' or '$' or '#';

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
