using DBAClientX;
using System.Data;
using System.Data.SqlClient;

public static class ParameterTypeConversionExample
{
    public static void Run()
    {
        using var sqlServer = new SqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 1
        };
        var parameterTypes = new Dictionary<string, SqlDbType>
        {
            ["@id"] = SqlDbType.Int
        };
        sqlServer.Query("SQL1", "master", true, "SELECT @id", parameters, parameterTypes: parameterTypes);
    }
}
