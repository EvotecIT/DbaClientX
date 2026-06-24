using System.Reflection;
using DBAClientX.Metadata;
using Xunit;

namespace DbaClientX.Tests;

public class ProviderMetadataQueryTests
{
    [Fact]
    public void DbaColumnInfo_ExposesGenerationMetadata()
    {
        var column = new DbaColumnInfo("dbo", "Users", "Id", "int")
        {
            IsIdentity = true,
            IdentityGeneration = "IDENTITY",
            GeneratedExpression = "lower([Name])",
            GeneratedKind = "COMPUTED"
        };

        Assert.True(column.IsIdentity);
        Assert.Equal("IDENTITY", column.IdentityGeneration);
        Assert.Equal("lower([Name])", column.GeneratedExpression);
        Assert.Equal("COMPUTED", column.GeneratedKind);
    }

    [Fact]
    public void DbaIndexInfo_ExposesVisibilityMetadata()
    {
        var index = new DbaIndexInfo("dbo", "Users", "IX_Users_Name")
        {
            IsVisible = false
        };

        Assert.False(index.IsVisible);
    }

    [Fact]
    public void ProviderColumnQueries_ProjectGenerationMetadata()
    {
        string[] queries =
        {
            GetQuery<DBAClientX.SqlServer>("SqlServerColumnsQuery"),
            GetQuery<DBAClientX.PostgreSql>("PostgreSqlColumnsQuery"),
            GetQuery<DBAClientX.MySql>("MySqlColumnsQuery"),
            GetQuery<DBAClientX.Oracle>("OracleColumnsQuery"),
            GetQuery<DBAClientX.SQLite>("SQLiteColumnsQuery")
        };

        foreach (string query in queries)
        {
            Assert.Contains("is_identity", query);
            Assert.Contains("identity_generation", query);
            Assert.Contains("generated_expression", query);
            Assert.Contains("generated_kind", query);
        }
    }

    [Fact]
    public void SqlServerAndPostgreSqlIndexQueries_ExcludeInactiveIndexes()
    {
        string sqlServer = GetQuery<DBAClientX.SqlServer>("SqlServerIndexesQuery");
        string postgreSql = GetQuery<DBAClientX.PostgreSql>("PostgreSqlIndexesQuery");

        Assert.Contains("i.is_disabled = 0", sqlServer);
        Assert.Contains("ix.indisvalid", postgreSql);
    }

    [Fact]
    public void SqlServerRoutineQuery_PreservesReturnTypeModifiers()
    {
        string routines = GetQuery<DBAClientX.SqlServer>("SqlServerRoutinesQuery");

        Assert.Contains("rp.max_length", routines);
        Assert.Contains("rp.precision", routines);
        Assert.Contains("rp.scale", routines);
        Assert.Contains("ty.name IN ('decimal', 'numeric')", routines);
        Assert.Contains("ty.name IN ('nvarchar', 'nchar')", routines);
        Assert.Contains("ty.name IN ('datetime2', 'datetimeoffset', 'time')", routines);
    }

    [Fact]
    public void MySqlQueries_PreserveFunctionalIndexesAndProcedureReturnNulls()
    {
        string tables = GetQuery<DBAClientX.MySql>("MySqlTablesQuery");
        string indexes = GetQuery<DBAClientX.MySql>("MySqlIndexesQuery");
        string indexesWithExpressions = GetQuery<DBAClientX.MySql>("MySqlIndexesWithExpressionsQuery");
        string indexesWithVisibility = GetQuery<DBAClientX.MySql>("MySqlIndexesWithVisibilityQuery");
        string indexesWithExpressionsAndVisibility = GetQuery<DBAClientX.MySql>("MySqlIndexesWithExpressionsAndVisibilityQuery");
        string columns = GetQuery<DBAClientX.MySql>("MySqlColumnsQuery");
        string foreignKeys = GetQuery<DBAClientX.MySql>("MySqlForeignKeysQuery");
        string expressionSupport = GetQuery<DBAClientX.MySql>("MySqlStatisticsExpressionSupportQuery");
        string visibilitySupport = GetQuery<DBAClientX.MySql>("MySqlStatisticsVisibilitySupportQuery");
        string routines = GetQuery<DBAClientX.MySql>("MySqlRoutinesQuery");

        Assert.Contains("DATABASE() IS NULL AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')", tables);
        Assert.Contains("DATABASE() IS NULL AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')", columns);
        Assert.Contains("DATABASE() IS NULL AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')", indexes);
        Assert.Contains("DATABASE() IS NULL AND kcu.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')", foreignKeys);
        Assert.Contains("DATABASE() IS NULL AND ROUTINE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')", routines);
        Assert.Contains("NULL AS is_visible", indexes);
        Assert.Contains("NULL AS expression", indexes);
        Assert.Contains("INFORMATION_SCHEMA.COLUMNS", expressionSupport);
        Assert.Contains("COLUMN_NAME = 'EXPRESSION'", expressionSupport);
        Assert.Contains("COLUMN_NAME = 'IS_VISIBLE'", visibilitySupport);
        Assert.Contains("EXPRESSION AS expression", indexesWithExpressions);
        Assert.Contains("CASE WHEN EXPRESSION IS NULL THEN COLUMN_NAME ELSE NULL END AS column_name", indexesWithExpressions);
        Assert.Contains("CASE WHEN IS_VISIBLE = 'YES' THEN 1 WHEN IS_VISIBLE = 'NO' THEN 0 ELSE NULL END AS is_visible", indexesWithVisibility);
        Assert.Contains("EXPRESSION AS expression", indexesWithExpressionsAndVisibility);
        Assert.Contains("CASE WHEN IS_VISIBLE = 'YES' THEN 1 WHEN IS_VISIBLE = 'NO' THEN 0 ELSE NULL END AS is_visible", indexesWithExpressionsAndVisibility);
        Assert.Contains("CASE WHEN ROUTINE_TYPE = 'FUNCTION' THEN NULLIF(DTD_IDENTIFIER, '') ELSE NULL END AS data_type", routines);
    }

    [Fact]
    public void OracleQueries_PreserveExpressionsCaseFiltersAndFunctionReturnTypes()
    {
        string tables = GetQuery<DBAClientX.Oracle>("OracleTablesQuery");
        string indexes = GetQuery<DBAClientX.Oracle>("OracleIndexesQuery");
        string foreignKeys = GetQuery<DBAClientX.Oracle>("OracleForeignKeysQuery");
        string routines = GetQuery<DBAClientX.Oracle>("OracleRoutinesQuery");

        Assert.Contains("nested = 'NO'", tables);
        Assert.Contains("secondary = 'N'", tables);
        Assert.Contains("iot_type <> 'IOT_OVERFLOW'", tables);
        Assert.Contains(":schemaNameExact IS NULL OR owner = :schemaNameExact OR owner = UPPER(:schemaNameNormalized)", tables);
        Assert.Contains("FROM all_mviews", tables);
        Assert.Contains("all_ind_expressions", indexes);
        Assert.Contains("ie.column_expression AS expression", indexes);
        Assert.Contains("ic.column_name AS column_name", indexes);
        Assert.DoesNotContain("CASE WHEN ie.column_expression IS NULL", indexes);
        Assert.Contains("i.status = 'VALID'", indexes);
        Assert.Contains("i.visibility = 'VISIBLE'", indexes);
        Assert.Contains(":tableNameExact IS NULL OR fk.table_name = :tableNameExact OR fk.table_name = UPPER(:tableNameNormalized)", foreignKeys);
        Assert.Contains(":schemaNameExact IS NULL OR object_info.owner = :schemaNameExact OR object_info.owner = UPPER(:schemaNameNormalized)", routines);
        Assert.Contains("result_argument.position = 0", routines);
        Assert.Contains("result_argument.data_type", routines);
    }

    [Fact]
    public void SQLiteColumns_FilterHiddenVirtualTableColumns()
    {
        string columns = GetQuery<DBAClientX.SQLite>("SQLiteColumnsQuery");
        string indexes = GetQuery<DBAClientX.SQLite>("SQLiteIndexesQuery");

        Assert.Contains("ti.hidden <> 1", columns);
        Assert.Contains("NULL AS is_visible", indexes);
        Assert.Contains("replace(replace(replace(im.sql, char(13), ' '), char(10), ' '), char(9), ' ')", indexes);
        Assert.Contains("ltrim(substr(im.sql", indexes);
    }

    [Fact]
    public void PostgreSqlQueries_PreserveIncludedIndexesDomainNullabilityAndTriggerState()
    {
        string columns = GetQuery<DBAClientX.PostgreSql>("PostgreSqlColumnsQuery");
        string indexes = GetQuery<DBAClientX.PostgreSql>("PostgreSqlIndexesQuery");
        string foreignKeys = GetQuery<DBAClientX.PostgreSql>("PostgreSqlForeignKeysQuery");

        Assert.Contains("elemns.nspname || '.' || elemtyp.typname || '[]'", columns);
        Assert.Contains("typ.typtype = 'd' AND typ.typnotnull", columns);
        Assert.Contains("cols.ordinality > ix.indnkeyatts AS is_included", indexes);
        Assert.DoesNotContain("cols.ordinality <= ix.indnkeyatts)\r\nORDER BY", indexes);
        Assert.Contains("bool_and(tg.tgenabled <> 'D') AS is_enabled", foreignKeys);
    }

    private static string GetQuery<T>(string fieldName)
    {
        FieldInfo? field = typeof(T).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        return Assert.IsType<string>(field!.GetRawConstantValue());
    }
}
