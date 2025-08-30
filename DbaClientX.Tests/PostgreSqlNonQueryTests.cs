using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Npgsql;
using NpgsqlTypes;
using DBAClientX;

namespace DbaClientX.Tests;

public class PostgreSqlNonQueryTests
{
    private class OutputDictionaryPostgreSql : DBAClientX.PostgreSql
    {
        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            using var command = new NpgsqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (NpgsqlParameter p in command.Parameters)
            {
                if (p.Direction != ParameterDirection.Input)
                {
                    p.Value = 7;
                }
            }
            UpdateOutputParameters(command, parameters);
            return 1;
        }

        public override int ExecuteNonQuery(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new NpgsqlParameter(), static (p, t) => p.NpgsqlDbType = t);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public void ExecuteNonQuery_UpdatesOutputParameters()
    {
        using var postgre = new OutputDictionaryPostgreSql();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };
        postgre.ExecuteNonQuery("h", "d", "u", "p", "q", parameters, parameterDirections: directions);
        Assert.Equal(7, parameters["@out"]);
    }
}
