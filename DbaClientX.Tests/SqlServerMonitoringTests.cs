using DBAClientX;
using DBAClientX.SqlServerMonitoring;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerMonitoringTests
{
    [Fact]
    public void BuildConnectionString_WithMonitoringOptions_KeepsEncryptionAndAppliesDiagnosticsSettings()
    {
        string connectionString = SqlServer.BuildConnectionString(
            "sql01",
            "master",
            integratedSecurity: true,
            port: 14330,
            ssl: true,
            trustServerCertificate: true,
            connectTimeoutSeconds: 7,
            applicationName: "unit-test");

        var builder = new SqlConnectionStringBuilder(connectionString);

        Assert.Equal("sql01,14330", builder.DataSource);
        Assert.Equal("master", builder.InitialCatalog);
        Assert.True(builder.IntegratedSecurity);
        Assert.True(builder.Encrypt);
        Assert.True(builder.TrustServerCertificate);
        Assert.Equal(7, builder.ConnectTimeout);
        Assert.Equal("unit-test", builder.ApplicationName);
    }

    [Fact]
    public void BuildConnectionString_WithInvalidTimeout_ThrowsArgumentOutOfRangeException()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SqlServer.BuildConnectionString("sql01", "master", true, connectTimeoutSeconds: 0));
    }

    [Fact]
    public void MonitoringOptions_Includes_ReturnsTrueOnlyForRequestedScopes()
    {
        var options = new SqlServerMonitoringOptions
        {
            Scope = SqlServerMonitoringScope.Connectivity | SqlServerMonitoringScope.DatabaseState
        };

        Assert.True(options.Includes(SqlServerMonitoringScope.Connectivity));
        Assert.True(options.Includes(SqlServerMonitoringScope.DatabaseState));
        Assert.False(options.Includes(SqlServerMonitoringScope.AgentJobs));
    }

    [Fact]
    public void MonitoringScope_All_IncludesAvailabilityGroups()
    {
        var options = new SqlServerMonitoringOptions
        {
            Scope = SqlServerMonitoringScope.All
        };

        Assert.True(options.Includes(SqlServerMonitoringScope.AvailabilityGroups));
    }

    [Fact]
    public void MonitoringTarget_DefaultsToIntegratedSecurityAndMaster()
    {
        var target = new SqlServerMonitoringTarget
        {
            ServerOrInstance = "sql01"
        };

        Assert.True(target.IntegratedSecurity);
        Assert.Equal("master", target.Database);
        Assert.Equal("DbaClientX.Monitoring", target.ApplicationName);
    }
}
