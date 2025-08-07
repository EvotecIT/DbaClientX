using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace DbaClientX.Tests;

public class SqlServerNonQueryTests
{
    private class CaptureParametersSqlServer : DBAClientX.SqlServer
    {
        public List<(string Name, object? Value, DbType Type)> Captured { get; } = new();

        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null)
        {
            var command = new SqlCommand(query);
            AddParameters(command, parameters, parameterTypes);
            foreach (DbParameter p in command.Parameters)
            {
                Captured.Add((p.ParameterName, p.Value, p.DbType));
            }
            return 1;
        }

        public override int ExecuteNonQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null, string? connectionString = null)
        {
            IDictionary<string, DbType>? dbTypes = null;
            if (parameterTypes != null)
            {
                dbTypes = new Dictionary<string, DbType>(parameterTypes.Count);
                foreach (var kv in parameterTypes)
                {
                    var p = new SqlParameter { SqlDbType = kv.Value };
                    dbTypes[kv.Key] = p.DbType;
                }
            }
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes);
        }
    }

    [Fact]
    public void ExecuteNonQuery_BindsParameters()
    {
        using var sqlServer = new CaptureParametersSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        sqlServer.ExecuteNonQuery("s", "db", true, "UPDATE t SET c=1 WHERE id=@id", parameters);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && (int)p.Value == 5);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && (string)p.Value == "test");
    }

    [Fact]
    public void ExecuteNonQuery_PreservesParameterTypes()
    {
        using var sqlServer = new CaptureParametersSqlServer();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };
        var types = new Dictionary<string, SqlDbType>
        {
            ["@id"] = SqlDbType.Int,
            ["@name"] = SqlDbType.NVarChar
        };

        sqlServer.ExecuteNonQuery("s", "db", true, "UPDATE t SET name=@name WHERE id=@id", parameters, parameterTypes: types);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Type == DbType.Int32);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Type == DbType.String);
    }

    private class FakeTransactionSqlServer : DBAClientX.SqlServer
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null, string? connectionString = null)
        {
            TransactionStarted = true;
        }

        public override void Commit()
        {
            if (!TransactionStarted) throw new DBAClientX.DbaTransactionException("No active transaction.");
            TransactionStarted = false;
        }

        public override void Rollback()
        {
            if (!TransactionStarted) throw new DBAClientX.DbaTransactionException("No active transaction.");
            TransactionStarted = false;
        }

        public override int ExecuteNonQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, string? username = null, string? password = null, string? connectionString = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return 0;
        }
    }

    [Fact]
    public void ExecuteNonQuery_WithTransactionNotStarted_Throws()
    {
        using var sqlServer = new FakeTransactionSqlServer();
        Assert.Throws<DBAClientX.DbaTransactionException>(() => sqlServer.ExecuteNonQuery("s", "db", true, "q", useTransaction: true));
    }

    [Fact]
    public void ExecuteNonQuery_UsesTransaction_WhenStarted()
    {
        using var sqlServer = new FakeTransactionSqlServer();
        sqlServer.BeginTransaction("s", "db", true);
        var ex = Record.Exception(() => sqlServer.ExecuteNonQuery("s", "db", true, "q", useTransaction: true));
        Assert.Null(ex);
    }
}

