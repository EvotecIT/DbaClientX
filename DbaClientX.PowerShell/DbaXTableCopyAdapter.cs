using System.Data;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DBAClientX.PowerShell;

internal sealed class DbaXTableCopyAdapter : IDbaTableCopySource, IDbaTableCopyDestination
{
    private readonly DbaXBulkProvider _provider;
    private readonly string _connectionString;
    private readonly string[] _orderBy;
    private readonly bool _allowUnordered;
    private readonly SqlServerBulkInsertOptions? _sqlServerOptions;

    public DbaXTableCopyAdapter(
        DbaXBulkProvider provider,
        string connectionString,
        IReadOnlyList<string>? orderBy = null,
        bool allowUnordered = false,
        SqlServerBulkInsertOptions? sqlServerOptions = null)
    {
        _provider = provider;
        _connectionString = connectionString;
        _orderBy = orderBy?.Where(static value => !string.IsNullOrWhiteSpace(value)).Select(static value => value.Trim()).ToArray()
            ?? Array.Empty<string>();
        _allowUnordered = allowUnordered;
        _sqlServerOptions = sqlServerOptions;
    }

    public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteCountAsync(definition.SourceName, cancellationToken);

    public async Task<DataTable> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
    {
        var query = BuildPageQuery(request.Definition.SourceName, request.Offset, request.PageSize);
        var table = await ExecuteTableAsync(query, cancellationToken).ConfigureAwait(false);
        table.TableName = request.Definition.DestinationName;
        return table;
    }

    public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        => ExecuteNonQueryAsync($"DELETE FROM {QuotePath(definition.DestinationName)}", cancellationToken);

    public async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        switch (_provider)
        {
            case DbaXBulkProvider.SqlServer:
                using (var sqlServer = new DBAClientX.SqlServer())
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
            case DbaXBulkProvider.PostgreSql:
                using (var postgreSql = new DBAClientX.PostgreSql())
                {
                    await postgreSql.BulkInsertAsync(_connectionString, page, definition.DestinationName, batchSize: options.BatchSize, bulkCopyTimeout: options.BulkCopyTimeout, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.MySql:
                using (var mySql = new DBAClientX.MySql())
                {
                    await mySql.BulkInsertAsync(_connectionString, page, definition.DestinationName, batchSize: options.BatchSize, bulkCopyTimeout: options.BulkCopyTimeout, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.Oracle:
                using (var oracle = new DBAClientX.Oracle())
                {
                    await oracle.BulkInsertAsync(_connectionString, page, definition.DestinationName, batchSize: options.BatchSize, bulkCopyTimeout: options.BulkCopyTimeout, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.SQLite:
                using (var sqlite = new DBAClientX.SQLite())
                {
                    await sqlite.BulkInsertWithConnectionStringAsync(ResolveSQLiteConnectionString(), page, definition.DestinationName, batchSize: options.BatchSize, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{_provider}' is not supported.");
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
            case DbaXBulkProvider.SqlServer:
                using (var sqlServer = new DBAClientX.SqlServer())
                {
                    return await sqlServer.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaXBulkProvider.PostgreSql:
                using (var postgreSql = new DBAClientX.PostgreSql())
                {
                    return await postgreSql.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaXBulkProvider.MySql:
                using (var mySql = new DBAClientX.MySql())
                {
                    return await mySql.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaXBulkProvider.Oracle:
                using (var oracle = new DBAClientX.Oracle())
                {
                    return await oracle.ExecuteScalarAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            case DbaXBulkProvider.SQLite:
                if (HasSQLiteConnectionString())
                {
                    return await ExecuteSQLiteScalarWithConnectionStringAsync(query, cancellationToken).ConfigureAwait(false);
                }

                using (var sqlite = new DBAClientX.SQLite())
                {
                    return await sqlite.ExecuteScalarAsync(ResolveSQLiteDatabase(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            default:
                throw new PSArgumentException($"Provider '{_provider}' is not supported.");
        }
    }

    private async Task<DataTable> ExecuteTableAsync(string query, CancellationToken cancellationToken)
    {
        object? result;
        switch (_provider)
        {
            case DbaXBulkProvider.SqlServer:
                using (var sqlServer = new DBAClientX.SqlServer { ReturnType = ReturnType.DataTable })
                {
                    result = await sqlServer.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.PostgreSql:
                using (var postgreSql = new DBAClientX.PostgreSql { ReturnType = ReturnType.DataTable })
                {
                    result = await postgreSql.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.MySql:
                using (var mySql = new DBAClientX.MySql { ReturnType = ReturnType.DataTable })
                {
                    result = await mySql.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.Oracle:
                using (var oracle = new DBAClientX.Oracle { ReturnType = ReturnType.DataTable })
                {
                    result = await oracle.QueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.SQLite:
                if (HasSQLiteConnectionString())
                {
                    return await ExecuteSQLiteTableWithConnectionStringAsync(query, cancellationToken).ConfigureAwait(false);
                }

                using (var sqlite = new DBAClientX.SQLite { ReturnType = ReturnType.DataTable })
                {
                    result = await sqlite.QueryAsync(ResolveSQLiteDatabase(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{_provider}' is not supported.");
        }

        return result as DataTable
            ?? throw new PSInvalidOperationException($"Provider '{_provider}' did not return a DataTable.");
    }

    private async Task ExecuteNonQueryAsync(string query, CancellationToken cancellationToken)
    {
        switch (_provider)
        {
            case DbaXBulkProvider.SqlServer:
                using (var sqlServer = new DBAClientX.SqlServer())
                {
                    await sqlServer.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.PostgreSql:
                using (var postgreSql = new DBAClientX.PostgreSql())
                {
                    await postgreSql.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.MySql:
                using (var mySql = new DBAClientX.MySql())
                {
                    await mySql.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.Oracle:
                using (var oracle = new DBAClientX.Oracle())
                {
                    await oracle.ExecuteNonQueryAsync(_connectionString, query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            case DbaXBulkProvider.SQLite:
                if (HasSQLiteConnectionString())
                {
                    await ExecuteSQLiteNonQueryWithConnectionStringAsync(query, cancellationToken).ConfigureAwait(false);
                    break;
                }

                using (var sqlite = new DBAClientX.SQLite())
                {
                    await sqlite.ExecuteNonQueryAsync(ResolveSQLiteDatabase(), query, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
                break;
            default:
                throw new PSArgumentException($"Provider '{_provider}' is not supported.");
        }
    }

    private string BuildPageQuery(string tableName, long offset, int pageSize)
    {
        var quotedTable = QuotePath(tableName);
        var orderBy = BuildOrderByClause();
        return _provider switch
        {
            DbaXBulkProvider.SqlServer or DbaXBulkProvider.Oracle
                => $"SELECT * FROM {quotedTable}{orderBy} OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY",
            _ => $"SELECT * FROM {quotedTable}{orderBy} LIMIT {pageSize} OFFSET {offset}"
        };
    }

    private string BuildOrderByClause()
    {
        if (_orderBy.Length > 0)
        {
            return " ORDER BY " + string.Join(", ", _orderBy.Select(QuotePath));
        }

        if (!_allowUnordered)
        {
            throw new PSArgumentException("OrderBy is required for deterministic paged table copy. Use -AllowUnordered for ad hoc copies where natural provider order is acceptable.");
        }

        return _provider switch
        {
            DbaXBulkProvider.SqlServer => " ORDER BY (SELECT NULL)",
            DbaXBulkProvider.Oracle => " ORDER BY 1",
            _ => string.Empty
        };
    }

    private string QuotePath(string identifierPath)
        => string.Join(".", SplitIdentifierPath(identifierPath).Select(QuoteIdentifier));

    private static IReadOnlyList<string> SplitIdentifierPath(string identifierPath)
    {
        if (string.IsNullOrWhiteSpace(identifierPath))
        {
            throw new PSArgumentException("Identifier cannot be null or whitespace.");
        }

        var parts = identifierPath.Split('.');
        if (parts.Any(static part => string.IsNullOrWhiteSpace(part)))
        {
            throw new PSArgumentException($"Identifier '{identifierPath}' contains an empty path segment.");
        }

        return parts.Select(static part => part.Trim()).ToArray();
    }

    private string QuoteIdentifier(string identifier)
    {
        if (identifier.IndexOfAny(new[] { ';', '\r', '\n', '\0' }) >= 0)
        {
            throw new PSArgumentException($"Identifier '{identifier}' contains unsupported characters.");
        }

        return _provider switch
        {
            DbaXBulkProvider.SqlServer => "[" + identifier.Replace("]", "]]") + "]",
            DbaXBulkProvider.MySql => "`" + identifier.Replace("`", "``") + "`",
            DbaXBulkProvider.Oracle => QuoteOracleIdentifier(identifier),
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

    private string ResolveSQLiteDatabase()
    {
        if (!HasSQLiteConnectionString())
        {
            return _connectionString;
        }

        var builder = new SqliteConnectionStringBuilder(_connectionString);
        return builder.DataSource;
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

    private string ResolveSQLiteCommandConnectionString()
    {
        var builder = new SqliteConnectionStringBuilder(_connectionString)
        {
            Pooling = false
        };
        return builder.ConnectionString;
    }

    private async Task<object?> ExecuteSQLiteScalarWithConnectionStringAsync(string query, CancellationToken cancellationToken)
    {
        using (var connection = new SqliteConnection(ResolveSQLiteCommandConnectionString()))
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task<DataTable> ExecuteSQLiteTableWithConnectionStringAsync(string query, CancellationToken cancellationToken)
    {
        using (var connection = new SqliteConnection(ResolveSQLiteCommandConnectionString()))
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

    private async Task ExecuteSQLiteNonQueryWithConnectionStringAsync(string query, CancellationToken cancellationToken)
    {
        using (var connection = new SqliteConnection(ResolveSQLiteCommandConnectionString()))
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            using (var command = connection.CreateCommand())
            {
                command.CommandText = query;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    internal static string GetProviderAlias(DbaXBulkProvider provider)
        => provider switch
        {
            DbaXBulkProvider.SqlServer => "sqlserver",
            DbaXBulkProvider.PostgreSql => "postgresql",
            DbaXBulkProvider.MySql => "mysql",
            DbaXBulkProvider.Oracle => "oracle",
            DbaXBulkProvider.SQLite => "sqlite",
            _ => throw new PSArgumentException($"Provider '{provider}' is not supported.")
        };

}
