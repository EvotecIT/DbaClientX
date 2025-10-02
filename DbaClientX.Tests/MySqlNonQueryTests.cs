using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;
using MySqlConnector;

namespace DbaClientX.Tests;

public class MySqlNonQueryTests
{
    private class CaptureParametersMySql : DBAClientX.MySql
    {
        public List<(string Name, object? Value, DbType Type)> Captured { get; } = new();

        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new MySqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (DbParameter p in command.Parameters)
            {
                Captured.Add((p.ParameterName, p.Value, p.DbType));
            }
            return Task.FromResult(1);
        }

        public override Task<int> ExecuteNonQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new MySqlParameter(), static (p, t) => p.MySqlDbType = t);
            return ExecuteNonQueryAsync(null!, null, query, parameters, cancellationToken, dbTypes, parameterDirections);
        }
    }

    private class OutputParameterMySqlAsync : DBAClientX.MySql
    {
        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new MySqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters["@out"].Value = 42;
            UpdateOutputParameters(command, parameters);
            return Task.FromResult(1);
        }

        public override Task<int> ExecuteNonQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new MySqlParameter(), static (p, t) => p.MySqlDbType = t);
            return ExecuteNonQueryAsync(null!, null, query, parameters, cancellationToken, dbTypes, parameterDirections);
        }
    }

    private class OutputParameterMySql : DBAClientX.MySql
    {
        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new MySqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters["@out"].Value = 42;
            UpdateOutputParameters(command, parameters);
            return 1;
        }

        public override int ExecuteNonQuery(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var dbTypes = DbTypeConverter.ConvertParameterTypes(parameterTypes, static () => new MySqlParameter(), static (p, t) => p.MySqlDbType = t);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public void ExecuteNonQuery_PopulatesOutputParameter()
    {
        using var mySql = new OutputParameterMySql();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };

        mySql.ExecuteNonQuery("h", "d", "u", "p", "UPDATE t SET c=1", parameters, parameterDirections: directions);

        Assert.Equal(42, parameters["@out"]);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_BindsParameters()
    {
        using var mySql = new CaptureParametersMySql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        await mySql.ExecuteNonQueryAsync("h", "d", "u", "p", "UPDATE t SET c=1 WHERE id=@id", parameters);

        Assert.Contains(mySql.Captured, p => p.Name == "@id" && p.Value is int v && v == 5);
        Assert.Contains(mySql.Captured, p => p.Name == "@name" && p.Value is string s && s == "test");
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PreservesParameterTypes()
    {
        using var mySql = new CaptureParametersMySql();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };
        var types = new Dictionary<string, MySqlDbType>
        {
            ["@id"] = MySqlDbType.Int32,
            ["@name"] = MySqlDbType.VarChar
        };

        await mySql.ExecuteNonQueryAsync("h", "d", "u", "p", "UPDATE t SET name=@name WHERE id=@id", parameters, parameterTypes: types);

        Assert.Contains(mySql.Captured, p => p.Name == "@id" && p.Type == DbType.Int32);
        Assert.Contains(mySql.Captured, p => p.Name == "@name" && p.Type == DbType.String);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PopulatesOutputParameter()
    {
        using var mySql = new OutputParameterMySqlAsync();
        var parameters = new Dictionary<string, object?>
        {
            ["@out"] = null
        };
        var directions = new Dictionary<string, ParameterDirection>
        {
            ["@out"] = ParameterDirection.Output
        };

        await mySql.ExecuteNonQueryAsync("h", "d", "u", "p", "UPDATE t SET c=1", parameters, parameterDirections: directions);

        Assert.Equal(42, parameters["@out"]);
    }
}
