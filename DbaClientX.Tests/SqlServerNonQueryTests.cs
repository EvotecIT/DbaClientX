using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Threading;

namespace DbaClientX.Tests;

public class SqlServerNonQueryTests
{
    private class CaptureParametersSqlServer : DBAClientX.SqlServer
    {
        public List<(string Name, object? Value, SqlDbType Type)> Captured { get; } = new();

        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new SqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is SqlParameter sqlParameter)
                {
                    Captured.Add((sqlParameter.ParameterName, sqlParameter.Value, sqlParameter.SqlDbType));
                }
            }
            return 1;
        }

        public override int ExecuteNonQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
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

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Value is int v && v == 5);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Value is string s && s == "test");
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

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Type == SqlDbType.Int);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Type == SqlDbType.NVarChar);
    }

    [Fact]
    public void ExecuteNonQuery_PreservesProviderSpecificParameterTypes()
    {
        using var sqlServer = new CaptureParametersSqlServer();
        var tvp = new DataTable();
        tvp.Columns.Add("Id", typeof(int));
        tvp.Rows.Add(1);
        var parameters = new Dictionary<string, object?>
        {
            ["@items"] = tvp,
            ["@xml"] = "<root />"
        };
        var types = new Dictionary<string, SqlDbType>
        {
            ["@items"] = SqlDbType.Structured,
            ["@xml"] = SqlDbType.Xml
        };

        sqlServer.ExecuteNonQuery("s", "db", true, "EXEC dbo.Test @items, @xml", parameters, parameterTypes: types);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@items" && p.Type == SqlDbType.Structured);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@xml" && p.Type == SqlDbType.Xml);
    }

    private class OutputParameterSqlServer : DBAClientX.SqlServer
    {
        protected override int ExecuteNonQuery(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new SqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters["@out"].Value = 7;
            UpdateOutputParameters(command, parameters);
            return 1;
        }

        public override int ExecuteNonQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(null!, null, query, parameters, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public void ExecuteNonQuery_PopulatesOutputParameter()
    {
        using var sqlServer = new OutputParameterSqlServer();
        var parameters = new Dictionary<string, object?> { ["@out"] = null };
        var directions = new Dictionary<string, ParameterDirection> { ["@out"] = ParameterDirection.Output };

        sqlServer.ExecuteNonQuery("s", "db", true, "UPDATE t SET c=1", parameters, parameterDirections: directions);

        Assert.Equal(7, parameters["@out"]);
    }

    private class FakeTransactionSqlServer : DBAClientX.SqlServer
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
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

        public override int ExecuteNonQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
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

    private class CaptureParametersSqlServerAsync : DBAClientX.SqlServer
    {
        public List<(string Name, object? Value, SqlDbType Type)> Captured { get; } = new();

        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new SqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            foreach (DbParameter p in command.Parameters)
            {
                if (p is SqlParameter sqlParameter)
                {
                    Captured.Add((sqlParameter.ParameterName, sqlParameter.Value, sqlParameter.SqlDbType));
                }
            }
            return Task.FromResult(1);
        }

        public override Task<int> ExecuteNonQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQueryAsync(null!, null, query, parameters, cancellationToken, dbTypes, parameterDirections);
        }
    }

    private class OutputParameterSqlServerAsync : DBAClientX.SqlServer
    {
        protected override Task<int> ExecuteNonQueryAsync(DbConnection connection, DbTransaction? transaction, string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            var command = new SqlCommand(query);
            AddParameters(command, parameters, parameterTypes, parameterDirections);
            command.Parameters["@out"].Value = 7;
            UpdateOutputParameters(command, parameters);
            return Task.FromResult(1);
        }

        public override Task<int> ExecuteNonQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQueryAsync(null!, null, query, parameters, cancellationToken, dbTypes, parameterDirections);
        }
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_BindsParameters()
    {
        using var sqlServer = new CaptureParametersSqlServerAsync();
        var parameters = new Dictionary<string, object?>
        {
            ["@id"] = 5,
            ["@name"] = "test"
        };

        await sqlServer.ExecuteNonQueryAsync("s", "db", true, "UPDATE t SET c=1 WHERE id=@id", parameters);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Value is int v && v == 5);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Value is string s && s == "test");
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PreservesParameterTypes()
    {
        using var sqlServer = new CaptureParametersSqlServerAsync();
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

        await sqlServer.ExecuteNonQueryAsync("s", "db", true, "UPDATE t SET name=@name WHERE id=@id", parameters, parameterTypes: types);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@id" && p.Type == SqlDbType.Int);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@name" && p.Type == SqlDbType.NVarChar);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PreservesProviderSpecificParameterTypes()
    {
        using var sqlServer = new CaptureParametersSqlServerAsync();
        var tvp = new DataTable();
        tvp.Columns.Add("Id", typeof(int));
        tvp.Rows.Add(1);
        var parameters = new Dictionary<string, object?>
        {
            ["@items"] = tvp,
            ["@xml"] = "<root />"
        };
        var types = new Dictionary<string, SqlDbType>
        {
            ["@items"] = SqlDbType.Structured,
            ["@xml"] = SqlDbType.Xml
        };

        await sqlServer.ExecuteNonQueryAsync("s", "db", true, "EXEC dbo.Test @items, @xml", parameters, parameterTypes: types);

        Assert.Contains(sqlServer.Captured, p => p.Name == "@items" && p.Type == SqlDbType.Structured);
        Assert.Contains(sqlServer.Captured, p => p.Name == "@xml" && p.Type == SqlDbType.Xml);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_PopulatesOutputParameter()
    {
        using var sqlServer = new OutputParameterSqlServerAsync();
        var parameters = new Dictionary<string, object?>
        {
            ["@out"] = null
        };
        var directions = new Dictionary<string, ParameterDirection>
        {
            ["@out"] = ParameterDirection.Output
        };

        await sqlServer.ExecuteNonQueryAsync("s", "db", true, "UPDATE t SET c=1", parameters, parameterDirections: directions);

        Assert.Equal(7, parameters["@out"]);
    }

    private class FakeTransactionSqlServerAsync : DBAClientX.SqlServer
    {
        public bool TransactionStarted { get; private set; }

        public override void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
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

        public override Task<int> ExecuteNonQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
        {
            if (useTransaction && !TransactionStarted) throw new DBAClientX.DbaTransactionException("Transaction has not been started.");
            return Task.FromResult(0);
        }
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_WithTransactionNotStarted_Throws()
    {
        using var sqlServer = new FakeTransactionSqlServerAsync();
        await Assert.ThrowsAsync<DBAClientX.DbaTransactionException>(() => sqlServer.ExecuteNonQueryAsync("s", "db", true, "q", useTransaction: true));
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_UsesTransaction_WhenStarted()
    {
        using var sqlServer = new FakeTransactionSqlServerAsync();
        sqlServer.BeginTransaction("s", "db", true);
        var ex = await Record.ExceptionAsync(() => sqlServer.ExecuteNonQueryAsync("s", "db", true, "q", useTransaction: true));
        Assert.Null(ex);
    }
}

