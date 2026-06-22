using System.Collections.Generic;
using System.Data;
using DBAClientX.Metadata;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private const string SQLiteDatabasesQuery = @"
PRAGMA database_list;";

    private const string SQLiteTablesQuery = @"
SELECT
    tl.schema AS schema_name,
    tl.name AS object_name,
    CASE WHEN tl.type = 'view' THEN 'View' ELSE 'Table' END AS object_kind
FROM pragma_table_list tl
WHERE tl.schema = 'main'
  AND (@schema IS NULL OR tl.schema = @schema)
  AND tl.type IN ('table', 'view', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND (@table IS NULL OR tl.name = @table)
  AND (@includeViews = 1 OR tl.type <> 'view')
ORDER BY tl.name;";

    private const string SQLiteColumnsQuery = @"
SELECT
    tl.schema AS schema_name,
    tl.name AS table_name,
    ti.name AS column_name,
    ti.type AS data_type,
    ti.cid + 1 AS ordinal_position,
    CASE WHEN ti.""notnull"" = 0 THEN 1 ELSE 0 END AS is_nullable,
    NULL AS max_length,
    NULL AS numeric_precision,
    NULL AS numeric_scale,
    ti.dflt_value AS default_expression
FROM pragma_table_list tl
INNER JOIN pragma_table_xinfo(tl.name) ti
WHERE tl.schema = 'main'
  AND (@schema IS NULL OR tl.schema = @schema)
  AND tl.type IN ('table', 'view', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND (@table IS NULL OR tl.name = @table)
ORDER BY tl.name, ti.cid;";

    private const string SQLiteIndexesQuery = @"
SELECT
    'main' AS schema_name,
    m.name AS table_name,
    il.name AS index_name,
    il.origin AS index_type,
    il.""unique"" AS is_unique,
    CASE WHEN il.origin = 'pk' THEN 1 ELSE 0 END AS is_primary_key,
    ii.name AS column_name,
    ii.seqno + 1 AS ordinal_position,
    CASE WHEN ii.""desc"" = 1 THEN 1 ELSE 0 END AS is_descending
FROM pragma_table_list tl
INNER JOIN sqlite_master m ON m.name = tl.name AND m.type = 'table'
INNER JOIN pragma_index_list(m.name) il
INNER JOIN pragma_index_xinfo(il.name) ii
WHERE tl.schema = 'main'
  AND (@schema IS NULL OR tl.schema = @schema)
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND ii.""key"" = 1
  AND (@table IS NULL OR m.name = @table)
UNION ALL
SELECT
    tl.schema AS schema_name,
    tl.name AS table_name,
    'pk_' || tl.name AS index_name,
    'pk' AS index_type,
    1 AS is_unique,
    1 AS is_primary_key,
    ti.name AS column_name,
    ti.pk AS ordinal_position,
    0 AS is_descending
FROM pragma_table_list tl
INNER JOIN pragma_table_xinfo(tl.name) ti
WHERE tl.schema = 'main'
  AND (@schema IS NULL OR tl.schema = @schema)
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND ti.pk > 0
  AND (@table IS NULL OR tl.name = @table)
  AND NOT EXISTS (
      SELECT 1
      FROM pragma_index_list(tl.name) pk
      WHERE pk.origin = 'pk'
  )
ORDER BY table_name, index_name, ordinal_position;";

    private const string SQLiteForeignKeysQuery = @"
SELECT
    tl.schema AS schema_name,
    tl.name AS table_name,
    'fk_' || tl.name || '_' || fk.id AS foreign_key_name,
    fk.""from"" AS column_name,
    tl.schema AS referenced_schema_name,
    fk.""table"" AS referenced_table_name,
    COALESCE(fk.""to"", refpk.name) AS referenced_column_name,
    fk.seq + 1 AS ordinal_position,
    fk.on_update AS update_rule,
    fk.on_delete AS delete_rule,
    NULL AS is_enabled,
    NULL AS is_trusted
FROM pragma_table_list tl
INNER JOIN pragma_foreign_key_list(tl.name) fk
LEFT JOIN pragma_table_xinfo(fk.""table"") refpk ON refpk.pk = fk.seq + 1
WHERE tl.schema = 'main'
  AND (@schema IS NULL OR tl.schema = @schema)
  AND tl.type IN ('table', 'virtual')
  AND tl.name NOT LIKE 'sqlite_%'
  AND (@table IS NULL OR tl.name = @table)
ORDER BY tl.name, fk.id, fk.seq;";

    private const string SQLiteRoutinesQuery = @"
SELECT
    'main' AS schema_name,
    NULL AS routine_name,
    'Unknown' AS routine_kind,
    NULL AS data_type,
    NULL AS definition,
    NULL AS is_system
WHERE 1 = 0;";

    /// <summary>Lists attached SQLite databases visible to the connection.</summary>
    public virtual IReadOnlyList<DbaDatabaseInfo> GetDatabases(string database)
        => ExecuteMetadata(database, SQLiteDatabasesQuery, MapDatabase);

    /// <summary>Lists SQLite tables and, optionally, views visible to the connection.</summary>
    public virtual IReadOnlyList<DbaTableInfo> GetTables(string database, string? schema = null, bool includeViews = true)
        => ExecuteMetadata(database, SQLiteTablesQuery, MapTable, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = null,
            ["@includeViews"] = includeViews ? 1 : 0
        });

    /// <summary>Lists SQLite columns visible to the connection.</summary>
    public virtual IReadOnlyList<DbaColumnInfo> GetColumns(string database, string? schema = null, string? table = null)
        => ExecuteMetadata(database, SQLiteColumnsQuery, MapColumn, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists SQLite indexes visible to the connection. Multi-column indexes return one row per indexed column.</summary>
    public virtual IReadOnlyList<DbaIndexInfo> GetIndexes(string database, string? schema = null, string? table = null)
        => ExecuteMetadata(database, SQLiteIndexesQuery, MapIndex, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists SQLite foreign keys. Multi-column keys return one row per column mapping.</summary>
    public virtual IReadOnlyList<DbaForeignKeyInfo> GetForeignKeys(string database, string? schema = null, string? table = null)
        => ExecuteMetadata(database, SQLiteForeignKeysQuery, MapForeignKey, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Returns an empty collection because SQLite does not expose stored routines.</summary>
    public virtual IReadOnlyList<DbaRoutineInfo> GetRoutines(string database, string? schema = null)
        => ExecuteMetadata(database, SQLiteRoutinesQuery, MapRoutine);

    private IReadOnlyList<T> ExecuteMetadata<T>(
        string database,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null)
    {
        ValidateDatabasePath(database);
        var connectionString = BuildOperationalConnectionString(database, readOnly: true);
        var (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction: false);
        if (dispose)
        {
            using (connection)
            {
                return ExecuteMappedQuery(connection, transaction, query, map, parameters: parameters);
            }
        }

        return ExecuteMappedQuery(connection, transaction, query, map, parameters: parameters);
    }

    private static DbaDatabaseInfo MapDatabase(IDataRecord record)
        => new(DbaMetadataReader.GetString(record, "file"))
        {
            Owner = DbaMetadataReader.GetNullableString(record, "name"),
            IsSystem = false
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
