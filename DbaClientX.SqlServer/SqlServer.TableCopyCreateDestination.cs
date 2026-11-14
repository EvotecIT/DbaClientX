using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    // sys.columns describes the source exactly: types with their lengths, precision and scale, collations, sparse,
    // identity seed and increment, and computed definitions. Alias types use their base type, which exists in
    // every database.
    private const string SourceColumnsQuery = @"SELECT c.name, COALESCE(TYPE_NAME(c.system_type_id), t.name) AS base_type, t.name AS user_type,
    t.is_assembly_type, SCHEMA_NAME(t.schema_id) AS type_schema, c.max_length, c.precision, c.scale, c.collation_name,
    c.is_nullable, c.is_sparse, c.is_identity, CONVERT(nvarchar(64), ic.seed_value) AS seed, CONVERT(nvarchar(64), ic.increment_value) AS increment,
    cc.definition, cc.is_persisted
FROM sys.columns AS c
JOIN sys.types AS t ON t.user_type_id = c.user_type_id
LEFT JOIN sys.identity_columns AS ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
WHERE c.object_id = OBJECT_ID(@source)
ORDER BY c.column_id;";

    /// <summary>
    /// Creates <paramref name="destinationTable"/> with the column definitions of the SQL Server table or view
    /// <paramref name="sourceTable"/> when the destination does not exist.
    /// </summary>
    /// <remarks>
    /// Columns keep their types, lengths, precision, scale, collations, nullability and sparse storage; identity
    /// columns keep their seed and increment, and computed columns their definitions. Renamed columns turn computed
    /// columns into ordinary columns of the same type, because the definitions name source columns. The table is
    /// marked as auto-created, like tables created by AutoCreateTable writes.
    /// </remarks>
    /// <returns><see langword="true"/> when the table was created; <see langword="false"/> when it already existed.</returns>
    public async Task<bool> CreateTableFromSourceAsync(
        string sourceConnectionString,
        string sourceTable,
        string destinationConnectionString,
        string destinationTable,
        IReadOnlyDictionary<string, string>? columnMappings = null,
        IReadOnlyCollection<string>? excludedColumns = null,
        CancellationToken cancellationToken = default)
    {
        ValidateConnectionString(sourceConnectionString);
        ValidateConnectionString(destinationConnectionString);
        var excluded = new HashSet<string>(excludedColumns ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
        IReadOnlyList<SourceColumnDefinition> columns = await ReadSourceColumnsAsync(sourceConnectionString, sourceTable, cancellationToken).ConfigureAwait(false);
        if (columns.Count == 0)
        {
            throw new InvalidOperationException($"Source '{sourceTable}' could not be described to create the destination table.");
        }

        IReadOnlyList<string> parts = DbaIdentifierPath.SplitSegments(destinationTable).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
        if (parts.Count is < 1 or > 3) throw new ArgumentException("Destination must be a table, schema.table or database.schema.table name.", nameof(destinationTable));
        string schema = parts.Count >= 2 ? parts[parts.Count - 2] : "dbo";
        string table = parts[parts.Count - 1];
        string quoted = QuoteSqlServerIdentifier(schema) + "." + QuoteSqlServerIdentifier(table);
        bool renamed = columnMappings is { Count: > 0 };

        var builder = new StringBuilder();
        builder.AppendLine("IF OBJECT_ID(@objectName, N'U') IS NULL");
        builder.AppendLine("BEGIN");
        if (!IsDefaultSchema(schema))
        {
            builder.AppendLine("    IF SCHEMA_ID(@schemaName) IS NULL BEGIN DECLARE @dbaxCreateSchema nvarchar(300) = N'CREATE SCHEMA ' + QUOTENAME(@schemaName); EXEC(@dbaxCreateSchema); END");
        }

        builder.Append("    EXEC(N'CREATE TABLE ").Append(quoted.Replace("'", "''")).AppendLine(" (");
        bool first = true;
        foreach (SourceColumnDefinition column in columns)
        {
            if (excluded.Contains(column.Name)) continue;
            string name = columnMappings != null && columnMappings.TryGetValue(column.Name, out string? mapped) ? mapped : column.Name;
            if (!first) builder.AppendLine(",");
            first = false;
            builder.Append("        ").Append(BuildColumnDefinition(QuoteSqlServerIdentifier(name), column, computed: !renamed).Replace("'", "''"));
        }

        if (first) throw new InvalidOperationException($"Every column of source '{sourceTable}' is excluded.");
        builder.AppendLine();
        builder.AppendLine("    );');");
        AppendAutoCreatedMarker(builder);
        builder.AppendLine("    SELECT CONVERT(bit, 1);");
        builder.AppendLine("END");
        builder.AppendLine("ELSE SELECT CONVERT(bit, 0);");

        using SqlConnection connection = CreateConnection(destinationConnectionString);
        await DBAClientX.Diagnostics.DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        if (parts.Count == 3) connection.ChangeDatabase(parts[0]);
        using SqlCommand command = connection.CreateCommand();
        command.CommandText = builder.ToString();
        command.Parameters.AddWithValue("@objectName", quoted);
        command.Parameters.AddWithValue("@schemaName", schema);
        ApplySetupCommandTimeout(command);
        object? created = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return created is true;
    }

    private async Task<IReadOnlyList<SourceColumnDefinition>> ReadSourceColumnsAsync(string connectionString, string sourceTable, CancellationToken cancellationToken)
    {
        IReadOnlyList<string> parts = DbaIdentifierPath.SplitSegments(sourceTable).Select(DbaIdentifierPath.UnquoteSegment).ToArray();
        if (parts.Count is < 1 or > 3) throw new ArgumentException("Source must be a table, schema.table or database.schema.table name.", nameof(sourceTable));
        using SqlConnection connection = CreateConnection(connectionString);
        await DBAClientX.Diagnostics.DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
        if (parts.Count == 3) connection.ChangeDatabase(parts[0]);
        using SqlCommand command = connection.CreateCommand();
        command.CommandText = SourceColumnsQuery;
        command.Parameters.AddWithValue("@source", string.Join(".", parts.Skip(parts.Count == 3 ? 1 : 0).Select(QuoteSqlServerIdentifier)));
        ApplyCommandTimeout(command);
        var columns = new List<SourceColumnDefinition>();
        using SqlDataReader reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            columns.Add(new SourceColumnDefinition(
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetBoolean(3),
                reader.GetString(4),
                reader.GetInt16(5),
                reader.GetByte(6),
                reader.GetByte(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetBoolean(9),
                reader.GetBoolean(10),
                reader.GetBoolean(11),
                reader.IsDBNull(12) ? null : reader.GetString(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetString(14),
                !reader.IsDBNull(15) && reader.GetBoolean(15)));
        }

        return columns;
    }

    private static string BuildColumnDefinition(string quotedName, SourceColumnDefinition column, bool computed)
    {
        if (computed && column.ComputedDefinition != null)
        {
            return quotedName + " AS " + column.ComputedDefinition + (column.IsPersisted ? " PERSISTED" : string.Empty);
        }

        var builder = new StringBuilder(quotedName).Append(' ').Append(GetSourceColumnTypeText(column));
        if (column.Collation != null) builder.Append(" COLLATE ").Append(column.Collation);
        if (column.IsSparse) builder.Append(" SPARSE");
        if (column.IsIdentity && column.IdentitySeed != null && column.IdentityIncrement != null)
        {
            builder.Append(" IDENTITY(").Append(column.IdentitySeed).Append(", ").Append(column.IdentityIncrement).Append(')');
        }

        return builder.Append(column.IsNullable ? " NULL" : " NOT NULL").ToString();
    }

    private static string GetSourceColumnTypeText(SourceColumnDefinition column)
    {
        if (column.IsAssemblyType)
        {
            // hierarchyid, geometry and geography exist in every database; other CLR types keep their bytes.
            return string.Equals(column.TypeSchema, "sys", StringComparison.OrdinalIgnoreCase) ? column.UserType : "varbinary(max)";
        }

        string type = column.BaseType;
        string Length(int value) => value == -1 ? "max" : value.ToString(CultureInfo.InvariantCulture);
        return type switch
        {
            "char" or "varchar" or "binary" or "varbinary" => $"{type}({Length(column.MaxLength)})",
            "nchar" or "nvarchar" => $"{type}({(column.MaxLength == -1 ? "max" : (column.MaxLength / 2).ToString(CultureInfo.InvariantCulture))})",
            "decimal" or "numeric" => $"{type}({column.Precision.ToString(CultureInfo.InvariantCulture)},{column.Scale.ToString(CultureInfo.InvariantCulture)})",
            "time" or "datetime2" or "datetimeoffset" => $"{type}({column.Scale.ToString(CultureInfo.InvariantCulture)})",
            "timestamp" => "rowversion",
            _ => type
        };
    }

    private sealed record SourceColumnDefinition(
        string Name,
        string BaseType,
        string UserType,
        bool IsAssemblyType,
        string TypeSchema,
        short MaxLength,
        byte Precision,
        byte Scale,
        string? Collation,
        bool IsNullable,
        bool IsSparse,
        bool IsIdentity,
        string? IdentitySeed,
        string? IdentityIncrement,
        string? ComputedDefinition,
        bool IsPersisted);
}
