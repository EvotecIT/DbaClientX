using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.DataMovement;
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
                "IF SCHEMA_ID(@schemaName) IS NULL BEGIN DECLARE @dbaxCreateSchema nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@schemaName); EXEC(@dbaxCreateSchema); END",
                new Dictionary<string, object?> { ["@schemaName"] = destination.SchemaName });
        }

        ExecuteBulkInsertSetupCommand(
            connection,
            transaction,
            BuildCreateTableCommand(table, destination, options.ColumnMappings),
            new Dictionary<string, object?> { ["@objectName"] = destination.QuotedFullName });
    }

    private void EnsureAutoCreatedDestinationTable(
        SqlConnection connection,
        SqlTransaction? transaction,
        IReadOnlyList<SqlServerBulkSourceColumn> columns,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        AutoSizedTextReader? sizing = null)
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
                "IF SCHEMA_ID(@schemaName) IS NULL BEGIN DECLARE @dbaxCreateSchema nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@schemaName); EXEC(@dbaxCreateSchema); END",
                new Dictionary<string, object?> { ["@schemaName"] = destination.SchemaName });
        }

        ExecuteBulkInsertSetupCommand(
            connection,
            transaction,
            BuildCreateTableCommand(columns, destination, sizing),
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
                    "IF SCHEMA_ID(@schemaName) IS NULL BEGIN DECLARE @dbaxCreateSchema nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@schemaName); EXEC(@dbaxCreateSchema); END",
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

    private async Task EnsureAutoCreatedDestinationTableAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        IReadOnlyList<SqlServerBulkSourceColumn> columns,
        string destinationTable,
        SqlServerBulkInsertOptions? options,
        CancellationToken cancellationToken,
        AutoSizedTextReader? sizing = null)
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
                    "IF SCHEMA_ID(@schemaName) IS NULL BEGIN DECLARE @dbaxCreateSchema nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@schemaName); EXEC(@dbaxCreateSchema); END",
                    new Dictionary<string, object?> { ["@schemaName"] = destination.SchemaName },
                    cancellationToken)
                .ConfigureAwait(false);
        }

        await ExecuteBulkInsertSetupCommandAsync(
                connection,
                transaction,
                BuildCreateTableCommand(columns, destination, sizing),
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
        ApplySetupCommandTimeout(command);
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
        ApplySetupCommandTimeout(command);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    // Widening an auto-created column to nvarchar(max) rewrites the table, so setup DDL is not cut off by the
    // provider's 30-second default. An explicitly configured CommandTimeout still applies.
    private void ApplySetupCommandTimeout(SqlCommand command)
    {
        if (TryGetCommandTimeout(out int timeout)) command.CommandTimeout = timeout;
        else command.CommandTimeout = 0;
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
        var autoSized = new List<KeyValuePair<string, int?>>();
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
            string columnType;
            if (IsAutoSizedText(column.DataType, column.MaxLength))
            {
                // A column without values keeps nvarchar(max) when created and is not widened by this write.
                int longest = GetLongestText(column);
                int? length = longest < 0 ? null : GetAutoSizedTextLength(longest);
                if (longest >= 0) autoSized.Add(new KeyValuePair<string, int?>(destinationColumnName, length));
                columnType = GetAutoSizedTextType(length);
            }
            else
            {
                columnType = GetSqlServerColumnType(column);
                AddDeclaredTextLength(autoSized, destinationColumnName, column.DataType, column.MaxLength);
            }

            builder.Append("        ")
                .Append(QuoteSqlServerIdentifier(destinationColumnName))
                .Append(' ')
                .Append(columnType)
                .Append(column.AllowDBNull ? " NULL" : " NOT NULL");

            if (index + 1 < table.Columns.Count)
            {
                builder.Append(',');
            }

            builder.AppendLine();
        }

        builder.AppendLine("    );");
        AppendAutoCreatedMarker(builder);
        builder.AppendLine("END");
        AppendWidenAutoSizedText(builder, autoSized, elseBranch: true);
        return builder.ToString();
    }

    private static string BuildCreateTableCommand(
        IReadOnlyList<SqlServerBulkSourceColumn> columns,
        SqlServerDestinationTable destination,
        AutoSizedTextReader? sizing)
    {
        var builder = new StringBuilder();
        var autoSized = new List<KeyValuePair<string, int?>>();
        builder.AppendLine("IF OBJECT_ID(@objectName, N'U') IS NULL");
        builder.AppendLine("BEGIN");
        builder.Append("    CREATE TABLE ").Append(destination.QuotedFullName).AppendLine();
        builder.AppendLine("    (");

        for (var index = 0; index < columns.Count; index++)
        {
            var column = columns[index];
            string columnType;
            if (sizing != null && sizing.TryGetLimit(column.Ordinal, out int? length, out bool observed))
            {
                if (observed) autoSized.Add(new KeyValuePair<string, int?>(column.DestinationName, length));
                columnType = GetAutoSizedTextType(length);
            }
            else
            {
                columnType = GetSqlServerReaderColumnType(column);
                AddDeclaredTextLength(autoSized, column.DestinationName, column.DataType, column.MaxLength);
            }

            builder.Append("        ")
                .Append(QuoteSqlServerIdentifier(column.DestinationName))
                .Append(' ')
                .Append(columnType)
                .Append(column.AllowDBNull ? " NULL" : " NOT NULL");

            if (index + 1 < columns.Count)
            {
                builder.Append(',');
            }

            builder.AppendLine();
        }

        builder.AppendLine("    );");
        AppendAutoCreatedMarker(builder);
        builder.AppendLine("END");
        AppendWidenAutoSizedText(builder, autoSized, elseBranch: true);
        return builder.ToString();
    }

    private static string GetSqlServerColumnType(DataColumn column)
    {
        var type = Nullable.GetUnderlyingType(column.DataType) ?? column.DataType;
        if (type == typeof(decimal) || type == typeof(System.Data.SqlTypes.SqlDecimal)) return GetSqlServerDecimalColumnType(column);
        int? maxLength = column.MaxLength is > 0 ? column.MaxLength : null;
        return GetSqlServerColumnType(type, maxLength);
    }

    private static string GetSqlServerDecimalColumnType(DataColumn column)
    {
        int integerDigits = 0, scale = 0;
        bool sampled = false;
        foreach (DataRow row in column.Table!.Rows)
        {
            if (row.RowState == DataRowState.Deleted || row.IsNull(column)) continue;
            var value = row[column] is System.Data.SqlTypes.SqlDecimal wide ? wide : new System.Data.SqlTypes.SqlDecimal((decimal)row[column]);
            integerDigits = Math.Max(integerDigits, value.Precision - value.Scale);
            scale = Math.Max(scale, value.Scale);
            sampled = true;
        }
        if (!sampled) return GetSqlServerDecimalColumnType();
        int precision = Math.Max(1, integerDigits + scale);
        if (precision > 38) throw new ArgumentException("Decimal values cannot fit one SQL Server decimal column without precision loss. Create an explicit destination schema.");
        return $"decimal({precision},{scale})";
    }

    private static string GetSqlServerReaderColumnType(SqlServerBulkSourceColumn column)
    {
        if (GetSqlServerSourceColumnType(column.SqlTypeName, column.MaxLength, column.NumericPrecision, column.NumericScale) is { } sourceType)
        {
            return sourceType;
        }

        if (column.DataType == typeof(decimal))
        {
            if (!column.NumericPrecision.HasValue || !column.NumericScale.HasValue)
                throw new ArgumentException("Auto-created reader decimal columns require NumericPrecision and NumericScale. Create the destination explicitly, or use the DataTable overload.");
            int precision = column.NumericPrecision.Value, scale = column.NumericScale.Value;
            if (precision is < 1 or > 38 || scale < 0 || scale > precision)
                throw new ArgumentException("Reader decimal precision/scale cannot be represented by SQL Server.");
            return $"decimal({precision},{scale})";
        }
        return GetSqlServerColumnType(column.DataType, column.MaxLength);
    }

    private static string GetSqlServerColumnType(Type dataType, int? maxLength)
    {
        var type = Nullable.GetUnderlyingType(dataType) ?? dataType;
        if (type == typeof(string))
        {
            return maxLength is > 0 and <= 4000
                ? $"nvarchar({maxLength.Value})"
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
            return GetSqlServerDecimalColumnType();
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

    private static string GetSqlServerDecimalColumnType()
        => "decimal(38,18)";

    private static string QuoteSqlServerIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("SQL Server identifier cannot be null or whitespace.", nameof(identifier));
        }

        return "[" + identifier.Replace("]", "]]") + "]";
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
            var segments = DbaIdentifierPath.SplitSegments(destinationTable);
            return segments.Count switch
            {
                1 => new SqlServerDestinationTable("dbo", DbaIdentifierPath.UnquoteSegment(segments[0])),
                2 => new SqlServerDestinationTable(DbaIdentifierPath.UnquoteSegment(segments[0]), DbaIdentifierPath.UnquoteSegment(segments[1])),
                _ => throw new ArgumentException("AutoCreateTable supports one-part or two-part SQL Server destination table names.", nameof(destinationTable))
            };
        }
    }
}
