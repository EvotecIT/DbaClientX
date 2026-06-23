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
    CASE
        WHEN DOMAIN_NAME IS NOT NULL THEN CONCAT(COALESCE(DOMAIN_SCHEMA, TABLE_SCHEMA), '.', DOMAIN_NAME)
        ELSE DATA_TYPE
    END AS data_type,
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
    o.name AS table_name,
    i.name AS index_name,
    i.type_desc AS index_type,
    CAST(i.is_unique AS bit) AS is_unique,
    CAST(i.is_primary_key AS bit) AS is_primary_key,
    c.name AS column_name,
    CASE WHEN ic.key_ordinal > 0 THEN ic.key_ordinal ELSE ic.index_column_id END AS ordinal_position,
    CAST(ic.is_descending_key AS bit) AS is_descending,
    i.filter_definition
FROM sys.indexes i
INNER JOIN sys.objects o ON o.object_id = i.object_id AND o.type IN ('U', 'V')
INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
LEFT JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND (ic.key_ordinal > 0 OR i.type IN (3, 4, 5, 6))
LEFT JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.index_id > 0
  AND i.name IS NOT NULL
  AND (@schema IS NULL OR s.name = @schema)
  AND (@table IS NULL OR o.name = @table)
ORDER BY s.name, o.name, i.name, CASE WHEN ic.key_ordinal > 0 THEN ic.key_ordinal ELSE ic.index_column_id END;";

    private const string SqlServerForeignKeysQuery = @"
SELECT
    ps.name AS schema_name,
    pt.name AS table_name,
    fk.name AS foreign_key_name,
    pc.name AS column_name,
    rs.name AS referenced_schema_name,
    rt.name AS referenced_table_name,
    rc.name AS referenced_column_name,
    fkc.constraint_column_id AS ordinal_position,
    fk.update_referential_action_desc AS update_rule,
    fk.delete_referential_action_desc AS delete_rule,
    CAST(CASE WHEN fk.is_disabled = 0 THEN 1 ELSE 0 END AS bit) AS is_enabled,
    CAST(CASE WHEN fk.is_not_trusted = 0 THEN 1 ELSE 0 END AS bit) AS is_trusted
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
INNER JOIN sys.tables pt ON pt.object_id = fk.parent_object_id
INNER JOIN sys.schemas ps ON ps.schema_id = pt.schema_id
INNER JOIN sys.columns pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id
INNER JOIN sys.tables rt ON rt.object_id = fk.referenced_object_id
INNER JOIN sys.schemas rs ON rs.schema_id = rt.schema_id
INNER JOIN sys.columns rc ON rc.object_id = rt.object_id AND rc.column_id = fkc.referenced_column_id
WHERE (@schema IS NULL OR ps.name = @schema)
  AND (@table IS NULL OR pt.name = @table)
ORDER BY ps.name, pt.name, fk.name, fkc.constraint_column_id;";

    private const string SqlServerRoutinesQuery = @"
SELECT
    s.name AS schema_name,
    o.name AS routine_name,
    CASE
        WHEN o.type IN ('P', 'PC', 'X') THEN 'Procedure'
        WHEN o.type IN ('FN', 'IF', 'TF', 'FS', 'FT') THEN 'Function'
        ELSE 'Unknown'
    END AS routine_kind,
    NULL AS data_type,
    m.definition,
    CAST(CASE WHEN o.is_ms_shipped = 1 OR s.name IN ('sys', 'INFORMATION_SCHEMA') THEN 1 ELSE 0 END AS bit) AS is_system
FROM sys.objects o
INNER JOIN sys.schemas s ON s.schema_id = o.schema_id
LEFT JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE o.type IN ('P', 'PC', 'X', 'FN', 'IF', 'TF', 'FS', 'FT')
  AND (@schema IS NULL OR s.name = @schema)
ORDER BY s.name, o.name;";

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

    /// <summary>
    /// Lists SQL Server foreign keys visible to the connection. Multi-column keys return one row per column mapping.
    /// </summary>
    public virtual IReadOnlyList<DbaForeignKeyInfo> GetForeignKeys(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, SqlServerForeignKeysQuery, MapForeignKey, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>
    /// Lists SQL Server procedures and functions visible to the connection.
    /// </summary>
    public virtual IReadOnlyList<DbaRoutineInfo> GetRoutines(string connectionString, string? schema = null)
        => ExecuteMetadata(connectionString, SqlServerRoutinesQuery, MapRoutine, new Dictionary<string, object?>
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
