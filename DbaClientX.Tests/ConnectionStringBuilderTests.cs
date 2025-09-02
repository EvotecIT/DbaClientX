using System.Data.SqlClient;
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
    public void PostgreSql_BuildConnectionString_CreatesExpectedValues()
    {
        var cs = DBAClientX.PostgreSql.BuildConnectionString("host", "db", "user", "pass");
        var builder = new NpgsqlConnectionStringBuilder(cs);
        Assert.Equal("host", builder.Host);
        Assert.Equal("db", builder.Database);
        Assert.Equal("user", builder.Username);
        Assert.Equal("pass", builder.Password);
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
    public void SQLite_BuildConnectionString_CreatesExpectedValues()
    {
        var cs = DBAClientX.SQLite.BuildConnectionString("data.db");
        var builder = new SqliteConnectionStringBuilder(cs);
        Assert.Equal("data.db", builder.DataSource);
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
}
