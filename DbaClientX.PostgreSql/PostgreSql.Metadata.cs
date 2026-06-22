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
    table_schema AS schema_name,
    table_name AS object_name,
    CASE WHEN table_type = 'VIEW' THEN 'View' ELSE 'Table' END AS object_kind
FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
  AND (@schema IS NULL OR table_schema = @schema)
  AND (@includeViews = true OR table_type = 'BASE TABLE')
ORDER BY table_schema, table_name;";

    private const string PostgreSqlColumnsQuery = @"
SELECT
    table_schema AS schema_name,
    table_name,
    column_name,
    data_type,
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
    false AS is_descending
FROM pg_index ix
INNER JOIN pg_class tbl ON tbl.oid = ix.indrelid
INNER JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
INNER JOIN pg_class idx ON idx.oid = ix.indexrelid
INNER JOIN pg_am am ON am.oid = idx.relam
LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS cols(attnum, ordinality) ON true
LEFT JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = cols.attnum
WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema')
  AND (@schema IS NULL OR ns.nspname = @schema)
  AND (@table IS NULL OR tbl.relname = @table)
ORDER BY ns.nspname, tbl.relname, idx.relname, cols.ordinality;";

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
            IsDescending = DbaMetadataReader.GetNullableBoolean(record, "is_descending")
        };

    private static DbaTableKind ParseTableKind(string value)
        => string.Equals(value, "View", StringComparison.OrdinalIgnoreCase) ? DbaTableKind.View : DbaTableKind.Table;
}
