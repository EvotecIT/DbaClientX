using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class OracleNonQueryTests
{
    private class OutputParameterOracle : DBAClientX.Oracle
    {
        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new OracleCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters[":out"].Value = 123;
            UpdateOutputParameters(command, parameters);
            return 1;
        }

        public override int ExecuteNonQuery(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
        }
    }

    private class OutputParameterOracleAsync : DBAClientX.Oracle
    {
        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new OracleCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters[":out"].Value = 123;
            UpdateOutputParameters(command, parameters);
            return Task.FromResult(1);
        }

        public override Task<int> ExecuteNonQueryAsync(string host, string serviceName, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, OracleDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQueryAsync(null!, null, query, parameters, cancellationToken, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public void ExecuteNonQuery_PopulatesOutputParameter()
    {
        using var oracle = new OutputParameterOracle();
        var parameters = new Dictionary<string, object?> { [":out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { [":out"] = ParameterDirection.Output };

        oracle.ExecuteNonQuery("h", "svc", "u", "p", "UPDATE t SET c = 1", parameters, parameterDirections: directions);

        Assert.Equal(123, parameters[":out"]);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PopulatesOutputParameter()
    {
        using var oracle = new OutputParameterOracleAsync();
        var parameters = new Dictionary<string, object?> { [":out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { [":out"] = ParameterDirection.Output };

        await oracle.ExecuteNonQueryAsync("h", "svc", "u", "p", "UPDATE t SET c = 1", parameters, parameterDirections: directions);

        Assert.Equal(123, parameters[":out"]);
    }
}
