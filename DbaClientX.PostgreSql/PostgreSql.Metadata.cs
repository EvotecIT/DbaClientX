using System.Collections.Generic;
using System.Data;
using DBAClientX.Metadata;
using Npgsql;

namespace DBAClientX;

public partial class PostgreSql
{
    private const string PostgreSqlDatabasesQuery = @"
SELECT
    datname AS database_name,
    pg_catalog.pg_get_userbyid(datdba) AS owner_name,
    datcollate AS collation_name,
    datistemplate AS is_system
FROM pg_database
WHERE datallowconn = true
ORDER BY datname;";

    private const string PostgreSqlTablesQuery = @"
SELECT
    ns.nspname AS schema_name,
    cls.relname AS object_name,
    CASE WHEN cls.relkind IN ('v', 'm') THEN 'View' ELSE 'Table' END AS object_kind
FROM pg_class cls
INNER JOIN pg_namespace ns ON ns.oid = cls.relnamespace
WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
  AND ns.nspname NOT LIKE 'pg\_%' ESCAPE '\'
  AND cls.relkind IN ('r', 'p', 'f', 'v', 'm')
  AND (@schema IS NULL OR ns.nspname = @schema)
  AND (@includeViews = true OR cls.relkind IN ('r', 'p', 'f'))
ORDER BY ns.nspname, cls.relname;";

    private const string PostgreSqlColumnsQuery = @"
SELECT
    table_schema AS schema_name,
    table_name,
    column_name,
    CASE
        WHEN data_type IN ('USER-DEFINED', 'ARRAY') THEN udt_schema || '.' || udt_name
        ELSE data_type
    END AS data_type,
    ordinal_position,
    CASE WHEN is_nullable = 'YES' THEN true ELSE false END AS is_nullable,
    character_maximum_length AS max_length,
    numeric_precision,
    numeric_scale,
    column_default AS default_expression
FROM information_schema.columns
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND (@schema IS NULL OR table_schema = @schema)
  AND (@table IS NULL OR table_name = @table)
ORDER BY table_schema, table_name, ordinal_position;";

    private const string PostgreSqlIndexesQuery = @"
SELECT
    ns.nspname AS schema_name,
    tbl.relname AS table_name,
    idx.relname AS index_name,
    am.amname AS index_type,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary_key,
    att.attname AS column_name,
    cols.ordinality AS ordinal_position,
    ((COALESCE(opts.indoption_value, 0) & 1) = 1) AS is_descending,
    pg_get_expr(ix.indpred, ix.indrelid) AS filter_definition
FROM pg_index ix
INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
INNER JOIN pg_am am ON am.oid = idx.relam
LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS cols(attnum, ordinality) ON true
LEFT JOIN LATERAL unnest(ix.indoption) WITH ORDINALITY AS opts(indoption_value, ordinality) ON opts.ordinality = cols.ordinality
LEFT JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = cols.attnum
WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
  AND ns.nspname NOT LIKE 'pg\_%' ESCAPE '\'
  AND (@schema IS NULL OR ns.nspname = @schema)
  AND (@table IS NULL OR tbl.relname = @table)
  AND (cols.ordinality IS NULL OR cols.ordinality <= ix.indnkeyatts)
ORDER BY ns.nspname, tbl.relname, idx.relname, cols.ordinality;";

    private const string PostgreSqlForeignKeysQuery = @"
SELECT
    ns.nspname AS schema_name,
    tbl.relname AS table_name,
    con.conname AS foreign_key_name,
    att.attname AS column_name,
    rns.nspname AS referenced_schema_name,
    rtbl.relname AS referenced_table_name,
    ratt.attname AS referenced_column_name,
    cols.ordinality AS ordinal_position,
    CASE con.confupdtype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
        ELSE con.confupdtype::text
    END AS update_rule,
    CASE con.confdeltype
        WHEN 'a' THEN 'NO ACTION'
        WHEN 'r' THEN 'RESTRICT'
        WHEN 'c' THEN 'CASCADE'
        WHEN 'n' THEN 'SET NULL'
        WHEN 'd' THEN 'SET DEFAULT'
        ELSE con.confdeltype::text
    END AS delete_rule,
    true AS is_enabled,
    con.convalidated AS is_trusted
FROM pg_constraint con
INNER JOIN pg_class tbl ON tbl.oid = con.conrelid
INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
INNER JOIN pg_class rtbl ON rtbl.oid = con.confrelid
INNER JOIN pg_namespace rns ON rns.oid = rtbl.relnamespace
LEFT JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS cols(attnum, ordinality) ON true
LEFT JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS refcols(attnum, ordinality) ON refcols.ordinality = cols.ordinality
LEFT JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = cols.attnum
LEFT JOIN pg_attribute ratt ON ratt.attrelid = rtbl.oid AND ratt.attnum = refcols.attnum
WHERE con.contype = 'f'
  AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
  AND (@schema IS NULL OR ns.nspname = @schema)
  AND (@table IS NULL OR tbl.relname = @table)
ORDER BY ns.nspname, tbl.relname, con.conname, cols.ordinality;";

    private const string PostgreSqlRoutinesQuery = @"
SELECT
    routine_schema AS schema_name,
    routine_name,
    CASE WHEN routine_type = 'PROCEDURE' THEN 'Procedure' WHEN routine_type = 'FUNCTION' THEN 'Function' ELSE 'Unknown' END AS routine_kind,
    data_type,
    routine_definition AS definition,
    false AS is_system
FROM information_schema.routines
WHERE routine_schema NOT IN ('pg_catalog', 'information_schema')
  AND (@schema IS NULL OR routine_schema = @schema)
ORDER BY routine_schema, routine_name;";

    /// <summary>Lists PostgreSQL databases visible to the connection.</summary>
    public virtual IReadOnlyList<DbaDatabaseInfo> GetDatabases(string connectionString)
        => ExecuteMetadata(connectionString, PostgreSqlDatabasesQuery, MapDatabase);

    /// <summary>Lists PostgreSQL tables and, optionally, views visible to the connection.</summary>
    public virtual IReadOnlyList<DbaTableInfo> GetTables(string connectionString, string? schema = null, bool includeViews = true)
        => ExecuteMetadata(connectionString, PostgreSqlTablesQuery, MapTable, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@includeViews"] = includeViews
        });

    /// <summary>Lists PostgreSQL columns visible to the connection.</summary>
    public virtual IReadOnlyList<DbaColumnInfo> GetColumns(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, PostgreSqlColumnsQuery, MapColumn, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists PostgreSQL indexes visible to the connection. Multi-column indexes return one row per indexed column.</summary>
    public virtual IReadOnlyList<DbaIndexInfo> GetIndexes(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, PostgreSqlIndexesQuery, MapIndex, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists PostgreSQL foreign keys visible to the connection. Multi-column keys return one row per column mapping.</summary>
    public virtual IReadOnlyList<DbaForeignKeyInfo> GetForeignKeys(string connectionString, string? schema = null, string? table = null)
        => ExecuteMetadata(connectionString, PostgreSqlForeignKeysQuery, MapForeignKey, new Dictionary<string, object?>
        {
            ["@schema"] = schema,
            ["@table"] = table
        });

    /// <summary>Lists PostgreSQL procedures and functions visible to the connection.</summary>
    public virtual IReadOnlyList<DbaRoutineInfo> GetRoutines(string connectionString, string? schema = null)
        => ExecuteMetadata(connectionString, PostgreSqlRoutinesQuery, MapRoutine, new Dictionary<string, object?>
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
        NpgsqlConnection? connection = null;
        NpgsqlTransaction? transaction = null;
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
