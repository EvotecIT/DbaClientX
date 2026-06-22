using System.Collections.Generic;
using System.Data;
using DBAClientX.Metadata;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    private const string SqlServerDatabasesQuery = @"
SELECT
    name AS database_name,
    SUSER_SNAME(owner_sid) AS owner_name,
    collation_name,
    CAST(CASE WHEN database_id <= 4 THEN 1 ELSE 0 END AS bit) AS is_system
FROM sys.databases
ORDER BY name;";

    private const string SqlServerTablesQuery = @"
SELECT
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS object_name,
    CASE WHEN TABLE_TYPE = 'VIEW' THEN 'View' ELSE 'Table' END AS object_kind
FROM INFORMATION_SCHEMA.TABLES
WHERE (@schema IS NULL OR TABLE_SCHEMA = @schema)
  AND (@includeViews = 1 OR TABLE_TYPE = 'BASE TABLE')
ORDER BY TABLE_SCHEMA, TABLE_NAME;";

    private const string SqlServerColumnsQuery = @"
SELECT
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    COLUMN_NAME AS column_name,
    DATA_TYPE AS data_type,
    ORDINAL_POSITION AS ordinal_position,
    CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END AS is_nullable,
    CHARACTER_MAXIMUM_LENGTH AS max_length,
    NUMERIC_PRECISION AS numeric_precision,
    NUMERIC_SCALE AS numeric_scale,
    COLUMN_DEFAULT AS default_expression
FROM INFORMATION_SCHEMA.COLUMNS
WHERE (@schema IS NULL OR TABLE_SCHEMA = @schema)
  AND (@table IS NULL OR TABLE_NAME = @table)
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;";

    private const string SqlServerIndexesQuery = @"
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc AS index_type,
    CAST(i.is_unique AS bit) AS is_unique,
    CAST(i.is_primary_key AS bit) AS is_primary_key,
    c.name AS column_name,
    ic.key_ordinal AS ordinal_position,
    CAST(ic.is_descending_key AS bit) AS is_descending
FROM sys.indexes i
INNER JOIN sys.tables t ON t.object_id = i.object_id
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
LEFT JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0
LEFT JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.index_id > 0
  AND i.name IS NOT NULL
  AND (@schema IS NULL OR s.name = @schema)
  AND (@table IS NULL OR t.name = @table)
ORDER BY s.name, t.name, i.name, ic.key_ordinal;";

    /// <summary>
    /// Lists SQL Server databases visible to the connection.
    /// </summary>
    public virtual IReadOnlyList<DbaDatabaseInfo> GetDatabases(string connectionString)
        => ExecuteMetadata(connectionString, SqlServerDatabasesQuery, MapDatabase);

    /// <summary>
    /// Lists SQL Server tables and, optionally, views visible to the connection.
    /// </summary>
    public virtual IReadOnlyList<DbaTableInfo> GetTables(string connectionString, string? schema = null, bool includeViews = true)
        => ExecuteMetadata(connectionString, SqlServerTablesQuery, MapTable, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@includeViews"] = includeViews ? 1 : 0
        });

    /// <summary>
    /// Lists SQL Server columns visible to the connection.
    /// </summary>
    public virtual IReadOnlyList<DbaColumnInfo> GetColumns(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, SqlServerColumnsQuery, MapColumn, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>
    /// Lists SQL Server indexes visible to the connection. Multi-column indexes return one row per indexed column.
    /// </summary>
    public virtual IReadOnlyList<DbaIndexInfo> GetIndexes(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, SqlServerIndexesQuery, MapIndex, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    private IReadOnlyList<T> ExecuteMetadata<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null)
    {
        ValidateConnectionString(connectionString);
        SqlConnection? connection = null;
        SqlTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
            return ExecuteMappedQuery(connection, transaction, query, map, parameters: parameters);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private static DbaDatabaseInfo MapDatabase(IDataRecord record)
        => new(DbaMetadataReader.GetString(record, "database_name"))
        {
            Owner = DbaMetadataReader.GetNullableString(record, "owner_name"),
            Collation = DbaMetadataReader.GetNullableString(record, "collation_name"),
            IsSystem = DbaMetadataReader.GetNullableBoolean(record, "is_system")
        };

    private static DbaTableInfo MapTable(IDataRecord record)
        => new(
            DbaMetadataReader.GetString(record, "schema_name"),
            DbaMetadataReader.GetString(record, "object_name"),
            ParseTableKind(DbaMetadataReader.GetString(record, "object_kind")));

    private static DbaColumnInfo MapColumn(IDataRecord record)
        => new(
            DbaMetadataReader.GetString(record, "schema_name"),
            DbaMetadataReader.GetString(record, "table_name"),
            DbaMetadataReader.GetString(record, "column_name"),
            DbaMetadataReader.GetString(record, "data_type"))
        {
            Ordinal = DbaMetadataReader.GetInt32(record, "ordinal_position"),
            IsNullable = DbaMetadataReader.GetNullableBoolean(record, "is_nullable"),
            MaxLength = DbaMetadataReader.GetNullableInt64(record, "max_length"),
            Precision = DbaMetadataReader.GetNullableInt32(record, "numeric_precision"),
            Scale = DbaMetadataReader.GetNullableInt32(record, "numeric_scale"),
            DefaultExpression = DbaMetadataReader.GetNullableString(record, "default_expression")
        };

    private static DbaIndexInfo MapIndex(IDataRecord record)
        => new(
            DbaMetadataReader.GetString(record, "schema_name"),
            DbaMetadataReader.GetString(record, "table_name"),
            DbaMetadataReader.GetString(record, "index_name"))
        {
            IndexType = DbaMetadataReader.GetNullableString(record, "index_type"),
            IsUnique = DbaMetadataReader.GetBoolean(record, "is_unique"),
            IsPrimaryKey = DbaMetadataReader.GetBoolean(record, "is_primary_key"),
            Column = DbaMetadataReader.GetNullableString(record, "column_name"),
            Ordinal = DbaMetadataReader.GetNullableInt32(record, "ordinal_position") ?? 0,
            IsDescending = DbaMetadataReader.GetNullableBoolean(record, "is_descending")
        };

    private static DbaTableKind ParseTableKind(string value)
        => string.Equals(value, "View", StringComparison.OrdinalIgnoreCase) ? DbaTableKind.View : DbaTableKind.Table;
}
