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
        string routines = GetQuery<DBAClientX.MySql>("MySqlRoutinesQuery");

        Assert.Contains("EXPRESSION AS expression", indexes);
        Assert.Contains("CASE WHEN EXPRESSION IS NULL THEN COLUMN_NAME ELSE NULL END AS column_name", indexes);
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
        Assert.Contains("all_ind_expressions", indexes);
        Assert.Contains("ie.column_expression AS expression", indexes);
        Assert.Contains(":tableNameExact IS NULL OR fk.table_name = :tableNameExact OR fk.table_name = UPPER(:tableNameNormalized)", foreignKeys);
        Assert.Contains("result_argument.position = 0", routines);
        Assert.Contains("result_argument.data_type", routines);
    }

    private static string GetQuery<T>(string fieldName)
    {
        FieldInfo? field = typeof(T).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        return Assert.IsType<string>(field!.GetRawConstantValue());
    }
}
