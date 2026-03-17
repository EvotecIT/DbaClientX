using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Xunit;

namespace DbaClientX.Tests;

public class ConnectionStringBuilderTests
{
    [Fact]
    public void MySql_BuildConnectionString_CreatesExpectedValues()
    {
        var cs = DBAClientX.MySql.BuildConnectionString("host", "db", "user", "pass");
        var builder = new MySqlConnectionStringBuilder(cs);
        Assert.Equal("host", builder.Server);
        Assert.Equal("db", builder.Database);
        Assert.Equal("user", builder.UserID);
        Assert.Equal("pass", builder.Password);
        Assert.Equal(MySqlSslMode.Required, builder.SslMode);
    }

    [Fact]
    public void MySql_BuildConnectionString_SetsPortAndSsl()
    {
        var cs = DBAClientX.MySql.BuildConnectionString("host", "db", "user", "pass", port: 3307, ssl: true);
        var builder = new MySqlConnectionStringBuilder(cs);
        Assert.Equal((uint)3307, builder.Port);
        Assert.Equal(MySqlSslMode.Required, builder.SslMode);
    }

    [Fact]
    public void MySql_BuildConnectionString_RejectsDisabledSsl()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.MySql.BuildConnectionString("host", "db", "user", "pass", ssl: false));
        Assert.Contains("require SSL", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySql_BuildConnectionString_RejectsBlankHost()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.MySql.BuildConnectionString(" ", "db", "user", "pass"));
        Assert.Equal("host", exception.ParamName);
    }

    [Fact]
    public void PostgreSql_BuildConnectionString_CreatesExpectedValues()
    {
        var cs = DBAClientX.PostgreSql.BuildConnectionString("host", "db", "user", "pass");
        var builder = new NpgsqlConnectionStringBuilder(cs);
        Assert.Equal("host", builder.Host);
        Assert.Equal("db", builder.Database);
        Assert.Equal("user", builder.Username);
        Assert.Equal("pass", builder.Password);
        Assert.Equal(SslMode.Require, builder.SslMode);
    }

    [Fact]
    public void PostgreSql_BuildConnectionString_SetsPortAndSsl()
    {
        var cs = DBAClientX.PostgreSql.BuildConnectionString("host", "db", "user", "pass", port: 5433, ssl: true);
        var builder = new NpgsqlConnectionStringBuilder(cs);
        Assert.Equal(5433, builder.Port);
        Assert.Equal(SslMode.Require, builder.SslMode);
    }

    [Fact]
    public void PostgreSql_BuildConnectionString_RejectsDisabledSsl()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.PostgreSql.BuildConnectionString("host", "db", "user", "pass", ssl: false));
        Assert.Contains("require SSL", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PostgreSql_BuildConnectionString_RejectsBlankDatabase()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.PostgreSql.BuildConnectionString("host", " ", "user", "pass"));
        Assert.Equal("database", exception.ParamName);
    }

    [Fact]
    public void SQLite_BuildConnectionString_CreatesExpectedValues()
    {
        var cs = DBAClientX.SQLite.BuildConnectionString("data.db");
        var builder = new SqliteConnectionStringBuilder(cs);
        Assert.Equal("data.db", builder.DataSource);
        Assert.False(builder.Pooling);
    }

    [Fact]
    public void SQLite_BuildConnectionString_WithBusyTimeout_RoundsToSeconds()
    {
        var cs = DBAClientX.SQLite.BuildConnectionString("data.db", readOnly: false, busyTimeoutMs: 1500);
        var builder = new SqliteConnectionStringBuilder(cs);
        Assert.Equal(2, builder.DefaultTimeout);
    }

    [Fact]
    public void SQLite_BuildReadOnlyConnectionString_SetsReadOnlyModeAndTimeout()
    {
        var cs = DBAClientX.SQLite.BuildReadOnlyConnectionString("data.db", busyTimeoutMs: 5000);
        var builder = new SqliteConnectionStringBuilder(cs);
        Assert.Equal(SqliteOpenMode.ReadOnly, builder.Mode);
        Assert.Equal(5, builder.DefaultTimeout);
        Assert.False(builder.Pooling);
    }

    [Fact]
    public void SQLite_BuildConnectionString_RejectsBlankDatabasePath()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.SQLite.BuildConnectionString(" "));
        Assert.Equal("database", exception.ParamName);
    }

    [Fact]
    public void SQLite_BuildConnectionString_RejectsNegativeBusyTimeout()
    {
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => DBAClientX.SQLite.BuildConnectionString("data.db", readOnly: false, busyTimeoutMs: -1));
        Assert.Equal("busyTimeoutMs", exception.ParamName);
    }

    [Fact]
    public void SqlServer_BuildConnectionString_IntegratedSecurity()
    {
        var cs = DBAClientX.SqlServer.BuildConnectionString("srv", "db", true);
        var builder = new SqlConnectionStringBuilder(cs);
        Assert.Equal("srv", builder.DataSource);
        Assert.Equal("db", builder.InitialCatalog);
        Assert.True(builder.IntegratedSecurity);
    }

    [Fact]
    public void SqlServer_BuildConnectionString_WithCredentials()
    {
        var cs = DBAClientX.SqlServer.BuildConnectionString("srv", "db", false, "user", "pass");
        var builder = new SqlConnectionStringBuilder(cs);
        Assert.Equal("srv", builder.DataSource);
        Assert.Equal("db", builder.InitialCatalog);
        Assert.False(builder.IntegratedSecurity);
        Assert.Equal("user", builder.UserID);
        Assert.Equal("pass", builder.Password);
    }

    [Fact]
    public void SqlServer_BuildConnectionString_SetsPortAndSsl()
    {
        var cs = DBAClientX.SqlServer.BuildConnectionString("srv", "db", true, port: 1444, ssl: true);
        var builder = new SqlConnectionStringBuilder(cs);
        Assert.Equal("srv,1444", builder.DataSource);
        Assert.True(builder.Encrypt);
    }

    [Fact]
    public void SqlServer_BuildConnectionString_RejectsMissingUsernameWhenSqlAuthIsRequested()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.SqlServer.BuildConnectionString("srv", "db", false, username: " "));
        Assert.Equal("username", exception.ParamName);
    }

    [Fact]
    public void Oracle_BuildConnectionString_CreatesExpectedValues()
    {
        var cs = DBAClientX.Oracle.BuildConnectionString("host", "svc", "user", "pass");
        var builder = new OracleConnectionStringBuilder(cs);
        Assert.Equal("host/svc", builder.DataSource);
        Assert.Equal("user", builder.UserID);
        Assert.Equal("pass", builder.Password);
    }

    [Fact]
    public void Oracle_BuildConnectionString_SetsPort()
    {
        var cs = DBAClientX.Oracle.BuildConnectionString("host", "svc", "user", "pass", port: 1522);
        var builder = new OracleConnectionStringBuilder(cs);
        Assert.Equal("host:1522/svc", builder.DataSource);
    }

    [Fact]
    public void Oracle_BuildConnectionString_RejectsBlankServiceName()
    {
        var exception = Assert.Throws<ArgumentException>(() => DBAClientX.Oracle.BuildConnectionString("host", " ", "user", "pass"));
        Assert.Equal("serviceName", exception.ParamName);
    }
}
