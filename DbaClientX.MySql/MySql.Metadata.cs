using System.Collections.Generic;
using System.Data;
using DBAClientX.Metadata;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    private const string MySqlDatabasesQuery = @"
SELECT
    SCHEMA_NAME AS database_name,
    DEFAULT_COLLATION_NAME AS collation_name,
    CASE WHEN SCHEMA_NAME IN ('information_schema', 'mysql', 'performance_schema', 'sys') THEN 1 ELSE 0 END AS is_system
FROM INFORMATION_SCHEMA.SCHEMATA
ORDER BY SCHEMA_NAME;";

    private const string MySqlTablesQuery = @"
SELECT
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS object_name,
    CASE WHEN TABLE_TYPE = 'VIEW' THEN 'View' ELSE 'Table' END AS object_kind
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE())
  AND (@includeViews = 1 OR TABLE_TYPE = 'BASE TABLE')
ORDER BY TABLE_SCHEMA, TABLE_NAME;";

    private const string MySqlColumnsQuery = @"
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
WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE())
  AND (@table IS NULL OR TABLE_NAME = @table)
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;";

    private const string MySqlIndexesQuery = @"
SELECT
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    INDEX_NAME AS index_name,
    INDEX_TYPE AS index_type,
    CASE WHEN NON_UNIQUE = 0 THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN INDEX_NAME = 'PRIMARY' THEN 1 ELSE 0 END AS is_primary_key,
    COLUMN_NAME AS column_name,
    SEQ_IN_INDEX AS ordinal_position,
    CASE WHEN COLLATION = 'D' THEN 1 ELSE 0 END AS is_descending
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE())
  AND (@table IS NULL OR TABLE_NAME = @table)
ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;";

    /// <summary>Lists MySQL databases visible to the connection.</summary>
    public virtual IReadOnlyList<DbaDatabaseInfo> GetDatabases(string connectionString)
        => ExecuteMetadata(connectionString, MySqlDatabasesQuery, MapDatabase);

    /// <summary>Lists MySQL tables and, optionally, views visible to the connection.</summary>
    public virtual IReadOnlyList<DbaTableInfo> GetTables(string connectionString, string? schema = null, bool includeViews = true)
        => ExecuteMetadata(connectionString, MySqlTablesQuery, MapTable, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@includeViews"] = includeViews ? 1 : 0
        });

    /// <summary>Lists MySQL columns visible to the connection.</summary>
    public virtual IReadOnlyList<DbaColumnInfo> GetColumns(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, MySqlColumnsQuery, MapColumn, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists MySQL indexes visible to the connection. Multi-column indexes return one row per indexed column.</summary>
    public virtual IReadOnlyList<DbaIndexInfo> GetIndexes(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, MySqlIndexesQuery, MapIndex, new Dictionary<string, object?>
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
        MySqlConnection? connection = null;
        MySqlTransaction? transaction = null;
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
