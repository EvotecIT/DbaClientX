using System.Collections.Generic;
using System.Data;
using DBAClientX.Metadata;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    private const string OracleDatabasesQuery = @"
SELECT
    SYS_CONTEXT('USERENV', 'DB_NAME') AS database_name,
    SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS owner_name,
    NULL AS collation_name,
    0 AS is_system
FROM dual";

    private const string OracleTablesQuery = @"
SELECT owner AS schema_name, table_name AS object_name, 'Table' AS object_kind
FROM all_tables
WHERE owner = COALESCE(UPPER(:schemaTables), owner)
UNION ALL
SELECT owner AS schema_name, view_name AS object_name, 'View' AS object_kind
FROM all_views
WHERE :includeViews = 1
  AND owner = COALESCE(UPPER(:schemaViews), owner)
ORDER BY schema_name, object_name";

    private const string OracleColumnsQuery = @"
SELECT
    owner AS schema_name,
    table_name,
    column_name,
    data_type,
    column_id AS ordinal_position,
    CASE WHEN nullable = 'Y' THEN 1 ELSE 0 END AS is_nullable,
    CASE
        WHEN data_type IN ('CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') THEN char_length
        ELSE data_length
    END AS max_length,
    data_precision AS numeric_precision,
    data_scale AS numeric_scale,
    data_default AS default_expression
FROM all_tab_columns
WHERE (:schemaNameExact IS NULL OR owner = :schemaNameExact OR owner = UPPER(:schemaNameNormalized))
  AND (:tableNameExact IS NULL OR table_name = :tableNameExact OR table_name = UPPER(:tableNameNormalized))
ORDER BY owner, table_name, column_id";

    private const string OracleIndexesQuery = @"
SELECT
    i.owner AS schema_name,
    i.table_name,
    i.index_name,
    i.index_type,
    CASE WHEN i.uniqueness = 'UNIQUE' THEN 1 ELSE 0 END AS is_unique,
    CASE WHEN c.constraint_type = 'P' THEN 1 ELSE 0 END AS is_primary_key,
    ic.column_name,
    ic.column_position AS ordinal_position,
    CASE WHEN ic.descend = 'DESC' THEN 1 ELSE 0 END AS is_descending
FROM all_indexes i
INNER JOIN all_ind_columns ic ON ic.index_owner = i.owner AND ic.index_name = i.index_name AND ic.table_name = i.table_name
LEFT JOIN all_constraints c ON c.owner = i.owner AND c.index_name = i.index_name AND c.table_name = i.table_name AND c.constraint_type = 'P'
WHERE i.owner = COALESCE(UPPER(:schemaName), i.owner)
  AND i.table_name = COALESCE(UPPER(:tableName), i.table_name)
ORDER BY i.owner, i.table_name, i.index_name, ic.column_position";

    private const string OracleForeignKeysQuery = @"
SELECT
    fk.owner AS schema_name,
    fk.table_name,
    fk.constraint_name AS foreign_key_name,
    fkc.column_name,
    pk.owner AS referenced_schema_name,
    pk.table_name AS referenced_table_name,
    pkc.column_name AS referenced_column_name,
    fkc.position AS ordinal_position,
    NULL AS update_rule,
    fk.delete_rule,
    CASE WHEN fk.status = 'ENABLED' THEN 1 ELSE 0 END AS is_enabled,
    CASE WHEN fk.validated = 'VALIDATED' THEN 1 ELSE 0 END AS is_trusted
FROM all_constraints fk
INNER JOIN all_cons_columns fkc ON fkc.owner = fk.owner AND fkc.constraint_name = fk.constraint_name
INNER JOIN all_constraints pk ON pk.owner = fk.r_owner AND pk.constraint_name = fk.r_constraint_name
INNER JOIN all_cons_columns pkc ON pkc.owner = pk.owner AND pkc.constraint_name = pk.constraint_name AND pkc.position = fkc.position
WHERE fk.constraint_type = 'R'
  AND fk.owner = COALESCE(UPPER(:schemaName), fk.owner)
  AND fk.table_name = COALESCE(UPPER(:tableName), fk.table_name)
ORDER BY fk.owner, fk.table_name, fk.constraint_name, fkc.position";

    private const string OracleRoutinesQuery = @"
SELECT
    owner AS schema_name,
    object_name AS routine_name,
    CASE object_type
        WHEN 'PROCEDURE' THEN 'Procedure'
        WHEN 'FUNCTION' THEN 'Function'
        WHEN 'PACKAGE' THEN 'Package'
        ELSE 'Unknown'
    END AS routine_kind,
    NULL AS data_type,
    NULL AS definition,
    CASE WHEN owner IN ('SYS', 'SYSTEM') THEN 1 ELSE 0 END AS is_system
FROM all_objects
WHERE object_type IN ('PROCEDURE', 'FUNCTION', 'PACKAGE')
  AND owner = COALESCE(UPPER(:schemaName), owner)
ORDER BY owner, object_name";

    /// <summary>Returns the current Oracle database context visible to the connection.</summary>
    public virtual IReadOnlyList<DbaDatabaseInfo> GetDatabases(string connectionString)
        => ExecuteMetadata(connectionString, OracleDatabasesQuery, MapDatabase);

    /// <summary>Lists Oracle tables and, optionally, views visible to the connection.</summary>
    public virtual IReadOnlyList<DbaTableInfo> GetTables(string connectionString, string? schema = null, bool includeViews = true)
        => ExecuteMetadata(connectionString, OracleTablesQuery, MapTable, new Dictionary<string, object?>
        {
            [":schemaTables"] = schema,
            [":includeViews"] = includeViews ? 1 : 0,
            [":schemaViews"] = schema
        });

    /// <summary>Lists Oracle columns visible to the connection.</summary>
    public virtual IReadOnlyList<DbaColumnInfo> GetColumns(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, OracleColumnsQuery, MapColumn, new Dictionary<string, object?>
        {
            [":schemaNameExact"] = schema,
            [":schemaNameNormalized"] = schema,
            [":tableNameExact"] = table,
            [":tableNameNormalized"] = table
        });

    /// <summary>Lists Oracle indexes visible to the connection. Multi-column indexes return one row per indexed column.</summary>
    public virtual IReadOnlyList<DbaIndexInfo> GetIndexes(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, OracleIndexesQuery, MapIndex, new Dictionary<string, object?>
        {
            [":schemaName"] = schema,
            [":tableName"] = table
        });

    /// <summary>Lists Oracle foreign keys visible to the connection. Multi-column keys return one row per column mapping.</summary>
    public virtual IReadOnlyList<DbaForeignKeyInfo> GetForeignKeys(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, OracleForeignKeysQuery, MapForeignKey, new Dictionary<string, object?>
        {
            [":schemaName"] = schema,
            [":tableName"] = table
        });

    /// <summary>Lists Oracle procedures, functions, and packages visible to the connection.</summary>
    public virtual IReadOnlyList<DbaRoutineInfo> GetRoutines(string connectionString, string? schema = null)
        => ExecuteMetadata(connectionString, OracleRoutinesQuery, MapRoutine, new Dictionary<string, object?>
        {
            [":schemaName"] = schema
        });

    private IReadOnlyList<T> ExecuteMetadata<T>(
        string connectionString,
        string query,
        Func<IDataRecord, T> map,
        IDictionary<string, object?>? parameters = null)
    {
        ValidateConnectionString(connectionString);
        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
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
