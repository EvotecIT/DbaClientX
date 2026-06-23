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
    public void MySqlQueries_PreserveFunctionalIndexesAndProcedureReturnNulls()
    {
        string indexes = GetQuery<DBAClientX.MySql>("MySqlIndexesQuery");
        string indexesWithExpressions = GetQuery<DBAClientX.MySql>("MySqlIndexesWithExpressionsQuery");
        string expressionSupport = GetQuery<DBAClientX.MySql>("MySqlStatisticsExpressionSupportQuery");
        string routines = GetQuery<DBAClientX.MySql>("MySqlRoutinesQuery");

        Assert.Contains("NULL AS expression", indexes);
        Assert.Contains("INFORMATION_SCHEMA.COLUMNS", expressionSupport);
        Assert.Contains("COLUMN_NAME = 'EXPRESSION'", expressionSupport);
        Assert.Contains("EXPRESSION AS expression", indexesWithExpressions);
        Assert.Contains("CASE WHEN EXPRESSION IS NULL THEN COLUMN_NAME ELSE NULL END AS column_name", indexesWithExpressions);
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
        Assert.Contains("all_ind_expressions", indexes);
        Assert.Contains("ie.column_expression AS expression", indexes);
        Assert.Contains("i.status = 'VALID'", indexes);
        Assert.Contains(":tableNameExact IS NULL OR fk.table_name = :tableNameExact OR fk.table_name = UPPER(:tableNameNormalized)", foreignKeys);
        Assert.Contains("result_argument.position = 0", routines);
        Assert.Contains("result_argument.data_type", routines);
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
