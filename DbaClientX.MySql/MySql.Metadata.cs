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
    COLUMN_TYPE AS data_type,
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
    CASE WHEN COLLATION = 'D' THEN 1 ELSE 0 END AS is_descending,
    NULL AS filter_definition
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = COALESCE(@schema, DATABASE())
  AND (@table IS NULL OR TABLE_NAME = @table)
ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;";

    private const string MySqlForeignKeysQuery = @"
SELECT
    kcu.TABLE_SCHEMA AS schema_name,
    kcu.TABLE_NAME AS table_name,
    kcu.CONSTRAINT_NAME AS foreign_key_name,
    kcu.COLUMN_NAME AS column_name,
    kcu.REFERENCED_TABLE_SCHEMA AS referenced_schema_name,
    kcu.REFERENCED_TABLE_NAME AS referenced_table_name,
    kcu.REFERENCED_COLUMN_NAME AS referenced_column_name,
    kcu.ORDINAL_POSITION AS ordinal_position,
    rc.UPDATE_RULE AS update_rule,
    rc.DELETE_RULE AS delete_rule,
    NULL AS is_enabled,
    NULL AS is_trusted
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
LEFT JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
    AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
    AND rc.TABLE_NAME = kcu.TABLE_NAME
WHERE kcu.TABLE_SCHEMA = COALESCE(@schema, DATABASE())
  AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
  AND (@table IS NULL OR kcu.TABLE_NAME = @table)
ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION;";

    private const string MySqlRoutinesQuery = @"
SELECT
    ROUTINE_SCHEMA AS schema_name,
    ROUTINE_NAME AS routine_name,
    CASE WHEN ROUTINE_TYPE = 'PROCEDURE' THEN 'Procedure' WHEN ROUTINE_TYPE = 'FUNCTION' THEN 'Function' ELSE 'Unknown' END AS routine_kind,
    DTD_IDENTIFIER AS data_type,
    ROUTINE_DEFINITION AS definition,
    CASE WHEN ROUTINE_SCHEMA IN ('information_schema', 'mysql', 'performance_schema', 'sys') THEN 1 ELSE 0 END AS is_system
FROM INFORMATION_SCHEMA.ROUTINES
WHERE ROUTINE_SCHEMA = COALESCE(@schema, DATABASE())
ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME;";

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

    /// <summary>Lists MySQL foreign keys visible to the connection. Multi-column keys return one row per column mapping.</summary>
    public virtual IReadOnlyList<DbaForeignKeyInfo> GetForeignKeys(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, MySqlForeignKeysQuery, MapForeignKey, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists MySQL procedures and functions visible to the connection.</summary>
    public virtual IReadOnlyList<DbaRoutineInfo> GetRoutines(string connectionString, string? schema = null)
        => ExecuteMetadata(connectionString, MySqlRoutinesQuery, MapRoutine, new Dictionary<string, object?>
        {
            ["@schema"] = schema
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
            IsDescending = DbaMetadataReader.GetNullableBoolean(record, "is_descending"),
            FilterDefinition = DbaMetadataReader.GetNullableString(record, "filter_definition")
        };

    private static DbaForeignKeyInfo MapForeignKey(IDataRecord record)
        => new(
            DbaMetadataReader.GetString(record, "schema_name"),
            DbaMetadataReader.GetString(record, "table_name"),
            DbaMetadataReader.GetString(record, "foreign_key_name"),
            DbaMetadataReader.GetString(record, "column_name"),
            DbaMetadataReader.GetString(record, "referenced_schema_name"),
            DbaMetadataReader.GetString(record, "referenced_table_name"),
            DbaMetadataReader.GetString(record, "referenced_column_name"))
        {
            Ordinal = DbaMetadataReader.GetInt32(record, "ordinal_position"),
            UpdateRule = DbaMetadataReader.GetNullableString(record, "update_rule"),
            DeleteRule = DbaMetadataReader.GetNullableString(record, "delete_rule"),
            IsEnabled = DbaMetadataReader.GetNullableBoolean(record, "is_enabled"),
            IsTrusted = DbaMetadataReader.GetNullableBoolean(record, "is_trusted")
        };

    private static DbaRoutineInfo MapRoutine(IDataRecord record)
        => new(
            DbaMetadataReader.GetString(record, "schema_name"),
            DbaMetadataReader.GetString(record, "routine_name"),
            ParseRoutineKind(DbaMetadataReader.GetString(record, "routine_kind")))
        {
            DataType = DbaMetadataReader.GetNullableString(record, "data_type"),
            Definition = DbaMetadataReader.GetNullableString(record, "definition"),
            IsSystem = DbaMetadataReader.GetNullableBoolean(record, "is_system")
        };

    private static DbaTableKind ParseTableKind(string value)
        => string.Equals(value, "View", StringComparison.OrdinalIgnoreCase) ? DbaTableKind.View : DbaTableKind.Table;

    private static DbaRoutineKind ParseRoutineKind(string value)
        => value.ToUpperInvariant() switch
        {
            "PROCEDURE" => DbaRoutineKind.Procedure,
            "FUNCTION" => DbaRoutineKind.Function,
            "PACKAGE" => DbaRoutineKind.Package,
            _ => DbaRoutineKind.Unknown
        };
}
