using System.Data;
using System.Data.Common;
using DBAClientX;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public class SqlServerDateTimeParameterTests
{
    private sealed class ParameterSqlServer : DBAClientX.SqlServer
    {
        public SqlParameter Bind(object value, IDictionary<string, SqlDbType>? providerTypes = null, IDictionary<string, DbType>? dbTypes = null)
        {
            using var command = new SqlCommand();
            AddParameters(command, new Dictionary<string, object?> { ["@p0"] = value }, dbTypes ?? ConvertParameterTypes(providerTypes));
            var parameter = (SqlParameter)command.Parameters[0];
            command.Parameters.Clear();
            return parameter;
        }
    }

    [Fact]
    public void AddParameters_DateTimeByDefault_UsesDateTime()
    {
        using var sqlServer = new ParameterSqlServer();

        Assert.Equal(SqlDbType.DateTime, sqlServer.Bind(new DateTime(2026, 9, 26, 10, 30, 0, 123)).SqlDbType);
    }

    [Fact]
    public void AddParameters_DateTime2Enabled_UsesDateTime2()
    {
        using var sqlServer = new ParameterSqlServer { UseDateTime2ForDateTimeParameters = true };

        var parameter = sqlServer.Bind(new DateTime(2026, 9, 26, 10, 30, 0, 123).AddTicks(4567));

        Assert.Equal(SqlDbType.DateTime2, parameter.SqlDbType);
        Assert.Equal(SqlDbType.Int, sqlServer.Bind(5).SqlDbType);
    }

    [Fact]
    public void AddParameters_DateTime2Enabled_ExplicitTypeWins()
    {
        using var sqlServer = new ParameterSqlServer { UseDateTime2ForDateTimeParameters = true };

        var parameter = sqlServer.Bind(DateTime.UtcNow, new Dictionary<string, SqlDbType> { ["@p0"] = SqlDbType.SmallDateTime });

        Assert.Equal(SqlDbType.SmallDateTime, parameter.SqlDbType);
        Assert.Equal(SqlDbType.Date, sqlServer.Bind(DateTime.UtcNow, dbTypes: new Dictionary<string, DbType> { ["@p0"] = DbType.Date }).SqlDbType);
    }
}
