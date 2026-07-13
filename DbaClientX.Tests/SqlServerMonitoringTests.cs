using DBAClientX;
using DBAClientX.SqlServerMonitoring;
using Microsoft.Data.SqlClient;
using Xunit;

namespace DbaClientX.Tests;

public class SqlServerMonitoringTests
{
    private sealed class TokenlessCancellationSqlServer : SqlServer
    {
        private readonly CancellationTokenSource _cancellationSource;

        public TokenlessCancellationSqlServer(CancellationTokenSource cancellationSource)
            => _cancellationSource = cancellationSource;

        protected override Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            _cancellationSource.Cancel();
            return Task.FromException(new OperationCanceledException());
        }
    }

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
    public void BuildConnectionString_WithLegacySignature_RemainsCallable()
    {
        string connectionString = SqlServer.BuildConnectionString(
            "sql01",
            "master",
            true,
            null,
            null,
            1433,
            true);

        var builder = new SqlConnectionStringBuilder(connectionString);

        Assert.Equal("sql01,1433", builder.DataSource);
        Assert.Equal("master", builder.InitialCatalog);
        Assert.True(builder.IntegratedSecurity);
        Assert.True(builder.Encrypt);
    }

    [Fact]
    public void NormalizeSqlDateTimeUtc_WithUnspecifiedKind_TreatsValueAsUtc()
    {
        var value = new DateTime(2026, 5, 25, 12, 30, 0, DateTimeKind.Unspecified);

        DateTime normalized = SqlServerMonitoringMappers.NormalizeSqlDateTimeUtc(value);

        Assert.Equal(DateTimeKind.Utc, normalized.Kind);
        Assert.Equal(value.Ticks, normalized.Ticks);
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
    public void AvailabilityGroupHealth_WithMissingState_IsNotHealthy()
    {
        var health = new SqlServerAvailabilityGroupHealth
        {
            AvailabilityGroupName = "ag01",
            ReplicaServerName = "sql01"
        };

        Assert.False(health.IsHealthy);
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

    [Fact]
    public async Task GetConnectionDiagnosticsAsync_WhenProviderThrowsTokenlessCancellation_NormalizesCallerCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        using var sqlServer = new TokenlessCancellationSqlServer(cancellation);
        var target = new SqlServerMonitoringTarget
        {
            ServerOrInstance = "sql01",
            TrustServerCertificate = true
        };

        var exception = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            sqlServer.GetConnectionDiagnosticsAsync(target, cancellation.Token));

        Assert.Equal(cancellation.Token, exception.CancellationToken);
        var providerException = Assert.IsType<OperationCanceledException>(exception.InnerException);
        Assert.Equal(default, providerException.CancellationToken);
    }
}
