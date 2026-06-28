using System.Data;
using System.Data.Common;
using DBAClientX.DataMovement;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// SQLite source and destination adapter for <see cref="DbaTableCopyEngine"/>.
/// </summary>
public sealed class SQLiteTableCopyAdapter : DbaProviderTableCopyAdapterBase
{
    /// <summary>
    /// Creates a SQLite table-copy adapter.
    /// </summary>
    public SQLiteTableCopyAdapter(
        string connectionString,
        IReadOnlyList<string>? defaultOrderByColumns = null,
        bool allowUnordered = false,
        bool treatMissingTablesAsEmpty = false)
        : base(DbaTableCopyProvider.SQLite, connectionString, defaultOrderByColumns, allowUnordered, treatMissingTablesAsEmpty)
    {
    }

    /// <summary>
    /// Creates a SQLite table-copy adapter from neutral provider options.
    /// </summary>
    public SQLiteTableCopyAdapter(DbaProviderTableCopyAdapterOptions options)
        : this(
            options?.ConnectionString ?? throw new ArgumentNullException(nameof(options)),
            options.DefaultOrderByColumns,
            options.AllowUnordered,
            options.TreatMissingTablesAsEmpty)
    {
        if (options.Provider != DbaTableCopyProvider.SQLite)
        {
            throw new ArgumentException("Options must target SQLite.", nameof(options));
        }
    }

    /// <inheritdoc />
    public override async Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
    {
        using var sqlite = new SQLite();
        await sqlite.BulkInsertWithConnectionStringAsync(
                ResolveSQLiteConnectionString(),
                page,
                NormalizeSQLiteBulkDestinationTableName(definition.DestinationName),
                batchSize: options.BatchSize,
                cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<object?> ExecuteScalarCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = connection.CreateCommand();
        command.CommandText = query;
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task<DataTable> ExecuteTableCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = connection.CreateCommand();
        command.CommandText = query;
        using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await ReadDataTableAsync(reader, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task ExecuteNonQueryCoreAsync(string query, CancellationToken cancellationToken)
    {
        using var connection = new SqliteConnection(ResolveSQLiteConnectionString());
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        using var command = connection.CreateCommand();
        command.CommandText = query;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private string ResolveSQLiteConnectionString()
    {
        if (!IsSQLiteConnectionString(ConnectionString))
        {
            return SQLite.BuildConnectionString(ConnectionString);
        }

        var translatedConnectionString = SQLite.TranslateSQLiteFullUri(ConnectionString);
        var builder = new SqliteConnectionStringBuilder(translatedConnectionString);
        if (!HasConnectionStringKey(translatedConnectionString, "Pooling"))
        {
            builder.Pooling = false;
        }

        return builder.ConnectionString;
    }

    private static bool HasConnectionStringKey(string connectionString, string key)
    {
        var builder = new DbConnectionStringBuilder
        {
            ConnectionString = connectionString
        };
        return builder.ContainsKey(key);
    }

    private static bool IsSQLiteConnectionString(string value)
        => SplitConnectionStringEntries(value).Any(static entry =>
        {
            var separator = entry.IndexOf('=');
            if (separator < 0)
            {
                return false;
            }

            var key = entry.Substring(0, separator).Trim();
            return IsSQLiteConnectionStringKey(key);
        });

    private static bool IsSQLiteConnectionStringKey(string key)
        => string.Equals(key, "Data Source", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "DataSource", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Filename", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "FullUri", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Mode", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Cache", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Password", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Pooling", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Foreign Keys", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Recursive Triggers", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Default Timeout", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Command Timeout", StringComparison.OrdinalIgnoreCase) ||
           string.Equals(key, "Vfs", StringComparison.OrdinalIgnoreCase);

    private static IEnumerable<string> SplitConnectionStringEntries(string connectionString)
    {
        var start = 0;
        var quote = '\0';
        for (var index = 0; index < connectionString.Length; index++)
        {
            var value = connectionString[index];
            if (quote == '\0')
            {
                if (value is '"' or '\'')
                {
                    quote = value;
                    continue;
                }

                if (value == ';')
                {
                    yield return connectionString.Substring(start, index - start);
                    start = index + 1;
                }

                continue;
            }

            if (value == quote)
            {
                if (index + 1 < connectionString.Length && connectionString[index + 1] == quote)
                {
                    index++;
                    continue;
                }

                quote = '\0';
            }
        }

        yield return connectionString.Substring(start);
    }

    private static async Task<DataTable> ReadDataTableAsync(SqliteDataReader reader, CancellationToken cancellationToken)
    {
        var table = new DataTable();
        var columnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            table.Columns.Add(GetUniqueColumnName(reader.GetName(i), i, columnNames), reader.GetFieldType(i));
        }

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = table.NewRow();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[i] = await reader.IsDBNullAsync(i, cancellationToken).ConfigureAwait(false)
                    ? DBNull.Value
                    : reader.GetValue(i);
            }

            table.Rows.Add(row);
        }

        return table;
    }

    private static string GetUniqueColumnName(string? columnName, int ordinal, HashSet<string> usedNames)
    {
        var baseName = string.IsNullOrEmpty(columnName) ? $"Column{ordinal + 1}" : columnName!;
        var name = baseName;
        var suffix = 1;
        while (!usedNames.Add(name))
        {
            name = $"{baseName}_{suffix++}";
        }

        return name;
    }
}
