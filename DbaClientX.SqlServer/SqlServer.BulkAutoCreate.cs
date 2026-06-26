using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    private void EnsureAutoCreatedDestinationTable(
        SqlConnection connection,
        SqlTransaction? transaction,
        DataTable table,
        string destinationTable,
        SqlServerBulkInsertOptions? options)
    {
        if (options?.AutoCreateTable != true)
        {
            return;
        }

        var destination = SqlServerDestinationTable.Parse(destinationTable);
        if (!IsDefaultSchema(destination.SchemaName))
        {
            ExecuteBulkInsertSetupCommand(
                connection,
                transaction,
                "IF SCHEMA_ID(@schemaName) IS NULL EXEC(N'CREATE SCHEMA ' + QUOTENAME(@schemaName));",
                new Dictionary<string, object?> { ["@schemaName"] = destination.SchemaName });
        }

        ExecuteBulkInsertSetupCommand(
            connection,
            transaction,
            BuildCreateTableCommand(table, destination, options.ColumnMappings),
            new Dictionary<string, object?> { ["@objectName"] = destination.QuotedFullName });
    }

    private async Task EnsureAutoCreatedDestinationTableAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        DataTable table,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        CancellationToken cancellationToken)
    {
        if (options?.AutoCreateTable != true)
        {
            return;
        }

        var destination = SqlServerDestinationTable.Parse(destinationTable);
        if (!IsDefaultSchema(destination.SchemaName))
        {
            await ExecuteBulkInsertSetupCommandAsync(
                    connection,
                    transaction,
                    "IF SCHEMA_ID(@schemaName) IS NULL EXEC(N'CREATE SCHEMA ' + QUOTENAME(@schemaName));",
                    new Dictionary<string, object?> { ["@schemaName"] = destination.SchemaName },
                    cancellationToken)
                .ConfigureAwait(false);
        }

        await ExecuteBulkInsertSetupCommandAsync(
                connection,
                transaction,
                BuildCreateTableCommand(table, destination, options.ColumnMappings),
                new Dictionary<string, object?> { ["@objectName"] = destination.QuotedFullName },
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static bool IsDefaultSchema(string schemaName)
        => string.Equals(schemaName, "dbo", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Executes SQL Server setup SQL required before provider bulk-copy operations.
    /// </summary>
    /// <param name="connection">Open SQL Server connection used by the bulk-copy operation.</param>
    /// <param name="transaction">Optional SQL Server transaction used by the bulk-copy operation.</param>
    /// <param name="commandText">SQL command text to execute.</param>
    /// <param name="parameters">Command parameters keyed by parameter name.</param>
    protected virtual void ExecuteBulkInsertSetupCommand(
        SqlConnection connection,
        SqlTransaction? transaction,
        string commandText,
        IReadOnlyDictionary<string, object?> parameters)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = commandText;
        AddParameters(command, parameters);
        command.ExecuteNonQuery();
    }

    /// <summary>
    /// Asynchronously executes SQL Server setup SQL required before provider bulk-copy operations.
    /// </summary>
    /// <param name="connection">Open SQL Server connection used by the bulk-copy operation.</param>
    /// <param name="transaction">Optional SQL Server transaction used by the bulk-copy operation.</param>
    /// <param name="commandText">SQL command text to execute.</param>
    /// <param name="parameters">Command parameters keyed by parameter name.</param>
    /// <param name="cancellationToken">Token used to cancel setup command execution.</param>
    /// <returns>A task that completes when the setup command has run.</returns>
    protected virtual async Task ExecuteBulkInsertSetupCommandAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string commandText,
        IReadOnlyDictionary<string, object?> parameters,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = commandText;
        AddParameters(command, parameters);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddParameters(SqlCommand command, IReadOnlyDictionary<string, object?> parameters)
    {
        foreach (var parameter in parameters)
        {
            command.Parameters.AddWithValue(parameter.Key, parameter.Value ?? DBNull.Value);
        }
    }

    private static string BuildCreateTableCommand(
        DataTable table,
        SqlServerDestinationTable destination,
        IDictionary<string, string>? columnMappings)
    {
        var builder = new StringBuilder();
        builder.AppendLine("IF OBJECT_ID(@objectName, N'U') IS NULL");
        builder.AppendLine("BEGIN");
        builder.Append("    CREATE TABLE ").Append(destination.QuotedFullName).AppendLine();
        builder.AppendLine("    (");

        for (var index = 0; index < table.Columns.Count; index++)
        {
            var column = table.Columns[index];
            var destinationColumnName = columnMappings?.TryGetValue(column.ColumnName, out var mappedColumn) == true
                ? mappedColumn
                : column.ColumnName;

            builder.Append("        ")
                .Append(QuoteSqlServerIdentifier(destinationColumnName))
                .Append(' ')
                .Append(GetSqlServerColumnType(column))
                .Append(column.AllowDBNull ? " NULL" : " NOT NULL");

            if (index + 1 < table.Columns.Count)
            {
                builder.Append(',');
            }

            builder.AppendLine();
        }

        builder.AppendLine("    );");
        builder.AppendLine("END");
        return builder.ToString();
    }

    private static string GetSqlServerColumnType(DataColumn column)
    {
        var type = Nullable.GetUnderlyingType(column.DataType) ?? column.DataType;
        if (type == typeof(string))
        {
            return column.MaxLength is > 0 and <= 4000
                ? $"nvarchar({column.MaxLength})"
                : "nvarchar(max)";
        }

        if (type == typeof(char))
        {
            return "nchar(1)";
        }

        if (type == typeof(bool))
        {
            return "bit";
        }

        if (type == typeof(byte))
        {
            return "tinyint";
        }

        if (type == typeof(short))
        {
            return "smallint";
        }

        if (type == typeof(int))
        {
            return "int";
        }

        if (type == typeof(long))
        {
            return "bigint";
        }

        if (type == typeof(float))
        {
            return "real";
        }

        if (type == typeof(double))
        {
            return "float";
        }

        if (type == typeof(decimal))
        {
            return "decimal(38,10)";
        }

        if (type == typeof(DateTime))
        {
            return "datetime2";
        }

        if (type == typeof(DateTimeOffset))
        {
            return "datetimeoffset";
        }

        if (type == typeof(TimeSpan) || string.Equals(type.FullName, "System.TimeOnly", StringComparison.Ordinal))
        {
            return "time";
        }

        if (type == typeof(Guid))
        {
            return "uniqueidentifier";
        }

        if (type == typeof(byte[]))
        {
            return "varbinary(max)";
        }

        if (string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
        {
            return "date";
        }

        return "nvarchar(max)";
    }

    private static string QuoteSqlServerIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("SQL Server identifier cannot be null or whitespace.", nameof(identifier));
        }

        return "[" + identifier.Trim().Replace("]", "]]") + "]";
    }

    private sealed class SqlServerDestinationTable
    {
        private SqlServerDestinationTable(string schemaName, string tableName)
        {
            SchemaName = schemaName;
            QuotedFullName = QuoteSqlServerIdentifier(schemaName) + "." + QuoteSqlServerIdentifier(tableName);
        }

        internal string SchemaName { get; }

        internal string QuotedFullName { get; }

        internal static SqlServerDestinationTable Parse(string destinationTable)
        {
            var segments = SplitIdentifierPath(destinationTable);
            return segments.Count switch
            {
                1 => new SqlServerDestinationTable("dbo", UnquoteIdentifierSegment(segments[0])),
                2 => new SqlServerDestinationTable(UnquoteIdentifierSegment(segments[0]), UnquoteIdentifierSegment(segments[1])),
                _ => throw new ArgumentException("AutoCreateTable supports one-part or two-part SQL Server destination table names.", nameof(destinationTable))
            };
        }

        private static IReadOnlyList<string> SplitIdentifierPath(string identifierPath)
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

        private static string UnquoteIdentifierSegment(string segment)
        {
            var trimmed = segment.Trim();
            if (trimmed.Length >= 2 && trimmed[0] == '"' && trimmed[trimmed.Length - 1] == '"')
            {
                return trimmed.Substring(1, trimmed.Length - 2).Replace("\"\"", "\"");
            }

            if (trimmed.Length >= 2 && trimmed[0] == '[' && trimmed[trimmed.Length - 1] == ']')
            {
                return trimmed.Substring(1, trimmed.Length - 2).Replace("]]", "]");
            }

            if (trimmed.Length >= 2 && trimmed[0] == '`' && trimmed[trimmed.Length - 1] == '`')
            {
                return trimmed.Substring(1, trimmed.Length - 2).Replace("``", "`");
            }

            return trimmed;
        }
    }
}
