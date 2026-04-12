using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class ProviderMappedQueryParityTests
{
    private sealed class OpenFailureSqlServer : DBAClientX.SqlServer
    {
        public int AsyncDisposeCalls { get; private set; }
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(SqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(SqlConnection connection)
            => DisposeCalls++;

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private sealed class CapturingSqlServer : DBAClientX.SqlServer
    {
        public int QueryAsyncCalls { get; private set; }
        public int ScalarAsyncCalls { get; private set; }

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
            => Task.CompletedTask;

        protected override ValueTask DisposeConnectionAsync(SqlConnection connection)
            => default;

        protected override Task<object?> ExecuteQueryAsync(
            DbConnection connection,
            DbTransaction? transaction,
            string query,
            IDictionary<string, object?>? parameters = null,
            CancellationToken cancellationToken = default,
            IDictionary<string, DbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            QueryAsyncCalls++;
            return Task.FromResult<object?>("query");
        }

        protected override Task<object?> ExecuteScalarAsync(
            DbConnection connection,
            DbTransaction? transaction,
            string query,
            IDictionary<string, object?>? parameters = null,
            CancellationToken cancellationToken = default,
            IDictionary<string, DbType>? parameterTypes = null,
            IDictionary<string, ParameterDirection>? parameterDirections = null)
        {
            ScalarAsyncCalls++;
            return Task.FromResult<object?>(42);
        }
    }

    private sealed class OpenFailureMySql : DBAClientX.MySql
    {
        public int AsyncDisposeCalls { get; private set; }
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(MySqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(MySqlConnection connection)
            => DisposeCalls++;

        protected override Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override ValueTask DisposeConnectionAsync(MySqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private sealed class OpenFailurePostgreSql : DBAClientX.PostgreSql
    {
        public int AsyncDisposeCalls { get; private set; }
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(NpgsqlConnection connection)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(NpgsqlConnection connection)
            => DisposeCalls++;

        protected override Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override ValueTask DisposeConnectionAsync(NpgsqlConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    private sealed class OpenFailureOracle : DBAClientX.Oracle
    {
        public int AsyncDisposeCalls { get; private set; }
        public int DisposeCalls { get; private set; }

        protected override void OpenConnection(OracleConnection connection)
            => throw new InvalidOperationException("boom");

        protected override void DisposeConnection(OracleConnection connection)
            => DisposeCalls++;

        protected override Task OpenConnectionAsync(OracleConnection connection, CancellationToken cancellationToken)
            => Task.FromException(new InvalidOperationException("boom"));

        protected override ValueTask DisposeConnectionAsync(OracleConnection connection)
        {
            AsyncDisposeCalls++;
            return default;
        }
    }

    [Fact]
    public async Task SqlServerQueryAsListAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            sql.QueryAsListAsync<int>("Server=.;Database=app;", "SELECT 1", null!));

        Assert.Equal(0, sql.AsyncDisposeCalls);
    }

    [Fact]
    public async Task MySqlQueryAsListAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var mysql = new OpenFailureMySql();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            mysql.QueryAsListAsync<int>("Server=dbhost;Database=app;User ID=user;Password=password;SslMode=Required", "SELECT 1", null!));

        Assert.Equal(0, mysql.AsyncDisposeCalls);
    }

    [Fact]
    public async Task OracleQueryAsListAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var oracle = new OpenFailureOracle();
        var connectionString = DBAClientX.Oracle.BuildConnectionString("dbhost", "svc", "user", "password");

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            oracle.QueryAsListAsync<int>(connectionString, "SELECT 1 FROM dual", null!));

        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public async Task SqliteQueryAsListAsync_WithNullMapper_ThrowsBeforeBuildingConnection()
    {
        using var sqlite = new DBAClientX.SQLite();

        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            sqlite.QueryAsListAsync<int>(" ", "SELECT 1", null!));
    }

    [Fact]
    public void SqlServerQueryStreamAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();

        Assert.Throws<ArgumentNullException>(() =>
            sql.QueryStreamAsync<int>("Server=.;Database=app;", "SELECT 1", null!));

        Assert.Equal(0, sql.AsyncDisposeCalls);
    }

    [Fact]
    public void MySqlQueryStreamAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var mysql = new OpenFailureMySql();

        Assert.Throws<ArgumentNullException>(() =>
            mysql.QueryStreamAsync<int>("Server=dbhost;Database=app;User ID=user;Password=password;SslMode=Required", "SELECT 1", null!));

        Assert.Equal(0, mysql.AsyncDisposeCalls);
    }

    [Fact]
    public void OracleQueryStreamAsync_WithNullMapper_ThrowsBeforeOpeningConnection()
    {
        using var oracle = new OpenFailureOracle();
        var connectionString = DBAClientX.Oracle.BuildConnectionString("dbhost", "svc", "user", "password");

        Assert.Throws<ArgumentNullException>(() =>
            oracle.QueryStreamAsync<int>(connectionString, "SELECT 1 FROM dual", null!));

        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public void SqliteQueryStreamAsync_WithNullMapper_ThrowsBeforeBuildingConnection()
    {
        using var sqlite = new DBAClientX.SQLite();

        Assert.Throws<ArgumentNullException>(() =>
            sqlite.QueryStreamAsync<int>(" ", "SELECT 1", null!));
    }

    [Fact]
    public async Task ConnectionStringQueryAsync_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        await Assert.ThrowsAsync<ArgumentException>(() => sql.QueryAsync("Server=.;", "SELECT 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => mysql.QueryAsync("Server=dbhost;Database=app;User ID=user;Password=password", "SELECT 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => postgreSql.QueryAsync("Host=dbhost;Database=app;Username=user;Password=password", "SELECT 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => oracle.QueryAsync("Data Source=dbhost/svc;User Id=user", "SELECT 1 FROM dual"));

        Assert.Equal(0, sql.AsyncDisposeCalls);
        Assert.Equal(0, mysql.AsyncDisposeCalls);
        Assert.Equal(0, postgreSql.AsyncDisposeCalls);
        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public async Task ConnectionStringExecuteScalarAsync_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        await Assert.ThrowsAsync<ArgumentException>(() => sql.ExecuteScalarAsync("Server=.;Database=app;Encrypt=False", "SELECT 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => mysql.ExecuteScalarAsync("Server=dbhost;Database=app;User ID=user;Password=password", "SELECT 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => postgreSql.ExecuteScalarAsync("Host=dbhost;Database=app;Username=user;Password=password", "SELECT 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => oracle.ExecuteScalarAsync("Data Source=dbhost/svc;User Id=user", "SELECT 1 FROM dual"));

        Assert.Equal(0, sql.AsyncDisposeCalls);
        Assert.Equal(0, mysql.AsyncDisposeCalls);
        Assert.Equal(0, postgreSql.AsyncDisposeCalls);
        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public async Task SqlServerQueryAsync_UsesQueryExecutionPath()
    {
        using var connectionStringSql = new CapturingSqlServer();

        var connectionStringResult = await connectionStringSql.QueryAsync(
            "Server=.;Database=app;Integrated Security=True;Encrypt=True;TrustServerCertificate=True",
            "SELECT 42");

        Assert.Equal("query", connectionStringResult);
        Assert.Equal(1, connectionStringSql.QueryAsyncCalls);
        Assert.Equal(0, connectionStringSql.ScalarAsyncCalls);

        using var hostSql = new CapturingSqlServer();

        var hostResult = await hostSql.QueryAsync(".", "app", true, "SELECT 42");

        Assert.Equal("query", hostResult);
        Assert.Equal(1, hostSql.QueryAsyncCalls);
        Assert.Equal(0, hostSql.ScalarAsyncCalls);
    }

    [Fact]
    public async Task SqlServerConnectionStringExecuteScalarAsync_UsesScalarExecutionPath()
    {
        using var sql = new CapturingSqlServer();

        var result = await sql.ExecuteScalarAsync(
            "Server=.;Database=app;Integrated Security=True;Encrypt=True;TrustServerCertificate=True",
            "SELECT 42");

        Assert.Equal(42, result);
        Assert.Equal(1, sql.ScalarAsyncCalls);
        Assert.Equal(0, sql.QueryAsyncCalls);
    }

    [Fact]
    public async Task ConnectionStringExecuteNonQueryAsync_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        await Assert.ThrowsAsync<ArgumentException>(() => sql.ExecuteNonQueryAsync("Server=.;Database=app;Encrypt=False", "UPDATE t SET c = 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => mysql.ExecuteNonQueryAsync("Server=dbhost;Database=app;User ID=user;Password=password", "UPDATE t SET c = 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => postgreSql.ExecuteNonQueryAsync("Host=dbhost;Database=app;Username=user;Password=password", "UPDATE t SET c = 1"));
        await Assert.ThrowsAsync<ArgumentException>(() => oracle.ExecuteNonQueryAsync("Data Source=dbhost/svc;User Id=user", "UPDATE t SET c = 1"));

        Assert.Equal(0, sql.AsyncDisposeCalls);
        Assert.Equal(0, mysql.AsyncDisposeCalls);
        Assert.Equal(0, postgreSql.AsyncDisposeCalls);
        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }

    [Fact]
    public void ConnectionStringExecuteNonQuery_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        Assert.Throws<ArgumentException>(() => sql.ExecuteNonQuery("Server=.;Database=app;Encrypt=False", "UPDATE t SET c = 1"));
        Assert.Throws<ArgumentException>(() => mysql.ExecuteNonQuery("Server=dbhost;Database=app;User ID=user;Password=password", "UPDATE t SET c = 1"));
        Assert.Throws<ArgumentException>(() => postgreSql.ExecuteNonQuery("Host=dbhost;Database=app;Username=user;Password=password", "UPDATE t SET c = 1"));
        Assert.Throws<ArgumentException>(() => oracle.ExecuteNonQuery("Data Source=dbhost/svc;User Id=user", "UPDATE t SET c = 1"));

        Assert.Equal(0, sql.DisposeCalls);
        Assert.Equal(0, mysql.DisposeCalls);
        Assert.Equal(0, postgreSql.DisposeCalls);
        Assert.Equal(0, oracle.DisposeCalls);
    }

    [Fact]
    public void ConnectionStringQuery_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        Assert.Throws<ArgumentException>(() => sql.Query("Server=.;Database=app;Encrypt=False", "SELECT 1"));
        Assert.Throws<ArgumentException>(() => mysql.Query("Server=dbhost;Database=app;User ID=user;Password=password", "SELECT 1"));
        Assert.Throws<ArgumentException>(() => postgreSql.Query("Host=dbhost;Database=app;Username=user;Password=password", "SELECT 1"));
        Assert.Throws<ArgumentException>(() => oracle.Query("Data Source=dbhost/svc;User Id=user", "SELECT 1 FROM dual"));

        Assert.Equal(0, sql.DisposeCalls);
        Assert.Equal(0, mysql.DisposeCalls);
        Assert.Equal(0, postgreSql.DisposeCalls);
        Assert.Equal(0, oracle.DisposeCalls);
    }

    [Fact]
    public void ConnectionStringExecuteScalar_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        Assert.Throws<ArgumentException>(() => sql.ExecuteScalar("Server=.;Database=app;Encrypt=False", "SELECT 1"));
        Assert.Throws<ArgumentException>(() => mysql.ExecuteScalar("Server=dbhost;Database=app;User ID=user;Password=password", "SELECT 1"));
        Assert.Throws<ArgumentException>(() => postgreSql.ExecuteScalar("Host=dbhost;Database=app;Username=user;Password=password", "SELECT 1"));
        Assert.Throws<ArgumentException>(() => oracle.ExecuteScalar("Data Source=dbhost/svc;User Id=user", "SELECT 1 FROM dual"));

        Assert.Equal(0, sql.DisposeCalls);
        Assert.Equal(0, mysql.DisposeCalls);
        Assert.Equal(0, postgreSql.DisposeCalls);
        Assert.Equal(0, oracle.DisposeCalls);
    }

    [Fact]
    public async Task ConnectionStringExecuteStoredProcedureAsync_RejectsInvalidSettingsBeforeOpeningConnection()
    {
        using var sql = new OpenFailureSqlServer();
        using var mysql = new OpenFailureMySql();
        using var postgreSql = new OpenFailurePostgreSql();
        using var oracle = new OpenFailureOracle();

        await Assert.ThrowsAsync<ArgumentException>(() => sql.ExecuteStoredProcedureAsync("Server=.;Database=app;Encrypt=False", "sp_test"));
        await Assert.ThrowsAsync<ArgumentException>(() => mysql.ExecuteStoredProcedureAsync("Server=dbhost;Database=app;User ID=user;Password=password", "sp_test"));
        await Assert.ThrowsAsync<ArgumentException>(() => postgreSql.ExecuteStoredProcedureAsync("Host=dbhost;Database=app;Username=user;Password=password", "sp_test"));
        await Assert.ThrowsAsync<ArgumentException>(() => oracle.ExecuteStoredProcedureAsync("Data Source=dbhost/svc;User Id=user", "sp_test"));

        Assert.Equal(0, sql.AsyncDisposeCalls);
        Assert.Equal(0, mysql.AsyncDisposeCalls);
        Assert.Equal(0, postgreSql.AsyncDisposeCalls);
        Assert.Equal(0, oracle.AsyncDisposeCalls);
    }
}
