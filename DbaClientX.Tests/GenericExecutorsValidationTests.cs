using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using NpgsqlTypes;
using Oracle.ManagedDataAccess.Client;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class GenericExecutorsValidationTests
{
    private sealed class CaptureSqlServer : DBAClientX.SqlServer
    {
        public string? LastConnectionString { get; private set; }
        public string? LastCommandText { get; private set; }

        public override Task<int> ExecuteNonQueryAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = query;
            return Task.FromResult(3);
        }

        public override Task<object?> ExecuteStoredProcedureAsync(
            string connectionString,
            string procedure,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, SqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = procedure;
            return Task.FromResult<object?>(null);
        }
    }

    private sealed class CapturePostgreSql : DBAClientX.PostgreSql
    {
        public string? LastConnectionString { get; private set; }
        public string? LastCommandText { get; private set; }

        public override Task<int> ExecuteNonQueryAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, NpgsqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = query;
            return Task.FromResult(4);
        }

        public override Task<object?> ExecuteStoredProcedureAsync(
            string connectionString,
            string procedure,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, NpgsqlDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = procedure;
            return Task.FromResult<object?>(null);
        }
    }

    private sealed class CaptureOracle : DBAClientX.Oracle
    {
        public string? LastConnectionString { get; private set; }
        public string? LastCommandText { get; private set; }

        public override Task<int> ExecuteNonQueryAsync(
            string connectionString,
            string query,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, OracleDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = query;
            return Task.FromResult(5);
        }

        public override Task<object?> ExecuteStoredProcedureAsync(
            string connectionString,
            string procedure,
            IDictionary<string, object?>? parameters = null,
            bool useTransaction = false,
            CancellationToken cancellationToken = default,
            IDictionary<string, OracleDbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            LastConnectionString = connectionString;
            LastCommandText = procedure;
            return Task.FromResult<object?>(null);
        }
    }

    [Fact]
    public async Task PostgreSqlGeneric_ExecuteSqlAsync_RejectsBlankConnectionString()
    {
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            DBAClientX.PostgreSqlGeneric.GenericExecutors.ExecuteSqlAsync(" ", "SELECT 1"));

        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public async Task SqlServerGeneric_ExecuteProcedureAsync_RejectsBlankProcedure()
    {
        var connectionString = DBAClientX.SqlServer.BuildConnectionString("srv", "db", true);
        var executorType = typeof(DBAClientX.SqlServer).Assembly.GetType("DBAClientX.SqlServerGeneric.GenericExecutors")!;
        var method = executorType.GetMethod("ExecuteProcedureAsync", BindingFlags.Public | BindingFlags.Static)!;

        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await (Task<int>)method.Invoke(null, new object?[] { connectionString, " ", null, default(System.Threading.CancellationToken) })!);

        Assert.Equal("procedure", exception.ParamName);
    }

    [Fact]
    public async Task OracleGeneric_ExecuteProcedureAsync_RejectsBlankConnectionString()
    {
        var executorType = typeof(DBAClientX.Oracle).Assembly.GetType("DBAClientX.OracleGeneric.GenericExecutors")!;
        var method = executorType.GetMethod(
            "ExecuteProcedureAsync",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(System.Threading.CancellationToken) },
            modifiers: null)!;

        var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await (Task<int>)method.Invoke(null, new object?[] { " ", "sp_test", null, default(System.Threading.CancellationToken) })!);

        Assert.Equal("connectionString", exception.ParamName);
    }

    [Fact]
    public async Task SQLiteGeneric_ExecuteSqlAsync_RejectsBlankDatabasePath()
    {
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            DBAClientX.SQLiteGeneric.GenericExecutors.ExecuteSqlAsync(" ", "SELECT 1"));

        Assert.Equal("connectionStringOrPath", exception.ParamName);
    }

    [Fact]
    public async Task SqlServerGeneric_ExecuteSqlAsync_PreservesOriginalConnectionString()
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = "srv,1444",
            InitialCatalog = "db",
            IntegratedSecurity = true,
            Encrypt = true,
            TrustServerCertificate = true,
            Pooling = false,
            ApplicationName = "DbaClientX.Tests"
        };
        var client = new CaptureSqlServer();
        var executorType = typeof(DBAClientX.SqlServer).Assembly.GetType("DBAClientX.SqlServerGeneric.GenericExecutors")!;
        var factoryProperty = executorType.GetProperty("ClientFactory", BindingFlags.NonPublic | BindingFlags.Static)!;
        var originalFactory = (Func<DBAClientX.SqlServer>)factoryProperty.GetValue(null)!;
        factoryProperty.SetValue(null, (Func<DBAClientX.SqlServer>)(() => client));

        try
        {
            var method = executorType.GetMethod(
                "ExecuteSqlAsync",
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(CancellationToken) },
                modifiers: null)!;
            var affected = await (Task<int>)method.Invoke(null, new object?[] { builder.ConnectionString, "UPDATE t SET c = 1", null, CancellationToken.None })!;

            Assert.Equal(3, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Equal("UPDATE t SET c = 1", client.LastCommandText);
        }
        finally
        {
            factoryProperty.SetValue(null, originalFactory);
        }
    }

    [Fact]
    public async Task SqlServerGeneric_ExecuteProcedureAsync_PreservesOriginalConnectionString()
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = "srv,1444",
            InitialCatalog = "db",
            IntegratedSecurity = true,
            Encrypt = true,
            TrustServerCertificate = true,
            Pooling = false,
            ApplicationName = "DbaClientX.Tests"
        };
        var client = new CaptureSqlServer();
        var executorType = typeof(DBAClientX.SqlServer).Assembly.GetType("DBAClientX.SqlServerGeneric.GenericExecutors")!;
        var factoryProperty = executorType.GetProperty("ClientFactory", BindingFlags.NonPublic | BindingFlags.Static)!;
        var originalFactory = (Func<DBAClientX.SqlServer>)factoryProperty.GetValue(null)!;
        factoryProperty.SetValue(null, (Func<DBAClientX.SqlServer>)(() => client));

        try
        {
            var method = executorType.GetMethod(
                "ExecuteProcedureAsync",
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(CancellationToken) },
                modifiers: null)!;
            var affected = await (Task<int>)method.Invoke(null, new object?[] { builder.ConnectionString, "dbo.sp_test", null, CancellationToken.None })!;

            Assert.Equal(0, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Equal("dbo.sp_test", client.LastCommandText);
        }
        finally
        {
            factoryProperty.SetValue(null, originalFactory);
        }
    }

    [Fact]
    public async Task PostgreSqlGeneric_ExecuteSqlAsync_PreservesOriginalConnectionString()
    {
        var connectionString = "Host=dbhost;Port=15432;Database=app;Username=user;Password=pass;SSL Mode=Require;Timeout=17;Pooling=false;Application Name=DbaClientX.Tests";
        var client = new CapturePostgreSql();
        var originalFactory = DBAClientX.PostgreSqlGeneric.GenericExecutors.ClientFactory;
        DBAClientX.PostgreSqlGeneric.GenericExecutors.ClientFactory = () => client;

        try
        {
            var affected = await DBAClientX.PostgreSqlGeneric.GenericExecutors.ExecuteSqlAsync(connectionString, "UPDATE t SET c = 1");

            Assert.Equal(4, affected);
            Assert.Equal(connectionString, client.LastConnectionString);
            Assert.Equal("UPDATE t SET c = 1", client.LastCommandText);
        }
        finally
        {
            DBAClientX.PostgreSqlGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }

    [Fact]
    public async Task PostgreSqlGeneric_ExecuteProcedureAsync_PreservesOriginalConnectionString()
    {
        var connectionString = "Host=dbhost;Port=15432;Database=app;Username=user;Password=pass;SSL Mode=Require;Timeout=17;Pooling=false;Application Name=DbaClientX.Tests";
        var client = new CapturePostgreSql();
        var originalFactory = DBAClientX.PostgreSqlGeneric.GenericExecutors.ClientFactory;
        DBAClientX.PostgreSqlGeneric.GenericExecutors.ClientFactory = () => client;

        try
        {
            var affected = await DBAClientX.PostgreSqlGeneric.GenericExecutors.ExecuteProcedureAsync(connectionString, "public.sp_test");

            Assert.Equal(0, affected);
            Assert.Equal(connectionString, client.LastConnectionString);
            Assert.Equal("public.sp_test", client.LastCommandText);
        }
        finally
        {
            DBAClientX.PostgreSqlGeneric.GenericExecutors.ClientFactory = originalFactory;
        }
    }

    [Fact]
    public async Task OracleGeneric_ExecuteSqlAsync_PreservesOriginalConnectionString()
    {
        var builder = new OracleConnectionStringBuilder
        {
            DataSource = "dbhost/svc",
            UserID = "user",
            Password = "pass",
            Pooling = false,
            ConnectionTimeout = 17
        };
        var client = new CaptureOracle();
        var executorType = typeof(DBAClientX.Oracle).Assembly.GetType("DBAClientX.OracleGeneric.GenericExecutors")!;
        var factoryProperty = executorType.GetProperty("ClientFactory", BindingFlags.NonPublic | BindingFlags.Static)!;
        var originalFactory = (Func<DBAClientX.Oracle>)factoryProperty.GetValue(null)!;
        factoryProperty.SetValue(null, (Func<DBAClientX.Oracle>)(() => client));

        try
        {
            var method = executorType.GetMethod(
                "ExecuteSqlAsync",
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(CancellationToken) },
                modifiers: null)!;
            var affected = await (Task<int>)method.Invoke(null, new object?[] { builder.ConnectionString, "UPDATE t SET c = 1", null, CancellationToken.None })!;

            Assert.Equal(5, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Equal("UPDATE t SET c = 1", client.LastCommandText);
        }
        finally
        {
            factoryProperty.SetValue(null, originalFactory);
        }
    }

    [Fact]
    public async Task OracleGeneric_ExecuteProcedureAsync_PreservesOriginalConnectionString()
    {
        var builder = new OracleConnectionStringBuilder
        {
            DataSource = "dbhost/svc",
            UserID = "user",
            Password = "pass",
            Pooling = false,
            ConnectionTimeout = 17
        };
        var client = new CaptureOracle();
        var executorType = typeof(DBAClientX.Oracle).Assembly.GetType("DBAClientX.OracleGeneric.GenericExecutors")!;
        var factoryProperty = executorType.GetProperty("ClientFactory", BindingFlags.NonPublic | BindingFlags.Static)!;
        var originalFactory = (Func<DBAClientX.Oracle>)factoryProperty.GetValue(null)!;
        factoryProperty.SetValue(null, (Func<DBAClientX.Oracle>)(() => client));

        try
        {
            var method = executorType.GetMethod(
                "ExecuteProcedureAsync",
                BindingFlags.Public | BindingFlags.Static,
                binder: null,
                types: new[] { typeof(string), typeof(string), typeof(IDictionary<string, object?>), typeof(CancellationToken) },
                modifiers: null)!;
            var affected = await (Task<int>)method.Invoke(null, new object?[] { builder.ConnectionString, "sp_test", null, CancellationToken.None })!;

            Assert.Equal(0, affected);
            Assert.Equal(builder.ConnectionString, client.LastConnectionString);
            Assert.Equal("sp_test", client.LastCommandText);
        }
        finally
        {
            factoryProperty.SetValue(null, originalFactory);
        }
    }
}
