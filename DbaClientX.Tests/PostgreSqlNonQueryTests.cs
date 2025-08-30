using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;
using Npgsql;
using NpgsqlTypes;

namespace DbaClientX.Tests;

public class PostgreSqlNonQueryTests
{
    private class OutputParameterPostgreSqlAsync : DBAClientX.PostgreSql
    {
        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new NpgsqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters["@out"].Value = 123;
            UpdateOutputParameters(command, parameters);
            return Task.FromResult(1);
        }

        public override Task<int> ExecuteNonQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new NpgsqlParameter(), static (p, t) => p.NpgsqlDbType = t);
            return ExecuteNonQueryAsync(null!, null, query, parameters, cancellationToken, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PopulatesOutputParameter()
    {
        using var postgreSql = new OutputParameterPostgreSqlAsync();
        var parameters = new Dictionary<string, object?>
        {
            ["@out"] = null
        };
        var directions = new Dictionary<string, ParameterDirection>
        {
            ["@out"] = ParameterDirection.Output
        };

        await postgreSql.ExecuteNonQueryAsync("h", "d", "u", "p", "UPDATE t SET c=1", parameters, parameterDirections: directions);

        Assert.Equal(123, parameters["@out"]);
    }
}
