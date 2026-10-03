using DBAClientX;
using DBAClientX.SqlServerMonitoring;
using Microsoft.Data.SqlClient;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public sealed class SqlServerWorkloadNativeTests
{
    [Fact]
    public async Task Workload_ReadsBoundedMetadataAndRetainsConstraintStatisticsAndResetContext()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.ExecuteAsync(@"
CREATE TABLE dbo.Observed (Id int NOT NULL CONSTRAINT PK_Observed PRIMARY KEY,
    Payload int NOT NULL CONSTRAINT UQ_Observed_Payload UNIQUE);
WITH numbers AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM numbers WHERE n < 1000)
INSERT dbo.Observed SELECT n, n * 2 FROM numbers OPTION (MAXRECURSION 0);
CREATE INDEX IX_Observed_Unread ON dbo.Observed(Payload, Id);
UPDATE STATISTICS dbo.Observed WITH FULLSCAN;
SELECT Payload FROM dbo.Observed WITH (INDEX(PK_Observed), FORCESEEK) WHERE Id = 400;");
        using var provider = new SqlServer();
        var snapshot = await provider.GetMonitoringSnapshotAsync(fixture.Target,
            new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.Workload, MaximumIndexUsageRows = 20 });
        Assert.Empty(snapshot.Errors);
        Assert.Equal(fixture.Database, snapshot.QueryStore!.DatabaseName);
        Assert.Equal("READ_WRITE", snapshot.QueryStore.DesiredState);
        Assert.Equal("READ_WRITE", snapshot.QueryStore.ActualState);
        Assert.Equal("ALL", snapshot.QueryStore.CaptureMode);
        Assert.False(snapshot.IndexUsage!.IsTruncated);
        Assert.Equal(3, snapshot.IndexUsage.Indexes.Count);
        Assert.NotNull(snapshot.IndexUsage.ServerStartTime);
        Assert.Equal(DateTimeKind.Unspecified, snapshot.IndexUsage.ServerStartTime.Value.Kind);
        var primary = Assert.Single(snapshot.IndexUsage.Indexes, index => index.IsPrimaryKey);
        Assert.True(primary.IsUnique);
        Assert.True(primary.UserSeeks > 0);
        Assert.Equal(1000L, primary.StatisticsRows);
        Assert.Equal(1000L, primary.StatisticsRowsSampled);
        Assert.NotNull(primary.StatisticsLastUpdated);
        Assert.Equal(DateTimeKind.Unspecified, primary.StatisticsLastUpdated.Value.Kind);
        Assert.True(Assert.Single(snapshot.IndexUsage.Indexes, index => index.IsUniqueConstraint).IsUnique);
        var limited = await provider.GetIndexUsageAsync(fixture.Target, maximumRows: 1);
        Assert.True(limited.IsTruncated);
        Assert.Single(limited.Indexes);
        Assert.Equal(primary.IndexId, limited.Indexes[0].IndexId);

        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Get-DbaXSqlServerMonitoring", typeof(CmdletGetDbaXSqlServerMonitoring), null));
        using var powerShell = PowerShell.Create(state);
        powerShell.AddCommand("Get-DbaXSqlServerMonitoring")
            .AddParameter("Server", fixture.Target.ServerOrInstance)
            .AddParameter("Database", fixture.Database)
            .AddParameter("Scope", SqlServerMonitoringScope.IndexUsage)
            .AddParameter("MaximumIndexUsageRows", 1)
            .AddParameter("TrustServerCertificate", fixture.Target.TrustServerCertificate);
        if (!fixture.Target.IntegratedSecurity)
            powerShell.AddParameter("Username", fixture.Target.Username).AddParameter("Password", fixture.Target.Password);
        var result = Assert.Single(powerShell.Invoke());
        Assert.False(powerShell.HadErrors, string.Join(Environment.NewLine, powerShell.Streams.Error));
        var cmdletSnapshot = Assert.IsType<SqlServerMonitoringSnapshot>(result.BaseObject);
        Assert.Empty(cmdletSnapshot.Errors);
        Assert.True(cmdletSnapshot.IndexUsage!.IsTruncated);
        Assert.Single(cmdletSnapshot.IndexUsage.Indexes);
    }

    [Fact]
    public async Task QueryStore_DisabledDatabaseReportsOffWithoutChangingConfiguration()
    {
        await using var fixture = await Fixture.CreateAsync(enableQueryStore: false);
        using var provider = new SqlServer();
        var snapshot = await provider.GetMonitoringSnapshotAsync(fixture.Target,
            new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.Workload });
        Assert.Empty(snapshot.Errors);
        Assert.Equal("OFF", snapshot.QueryStore!.DesiredState);
        Assert.Equal("OFF", snapshot.QueryStore.ActualState);
        Assert.Empty(snapshot.IndexUsage!.Indexes);
        Assert.Equal("OFF", (await provider.GetQueryStoreStateAsync(fixture.Target)).ActualState);
    }

    [Theory]
    [InlineData("master")]
    [InlineData("tempdb")]
    public async Task QueryStore_IneligibleSystemDatabaseIsUnsupportedRatherThanOff(string database)
    {
        var target = Fixture.CreateTarget(new SqlConnectionStringBuilder(Fixture.ApprovedConnection()) { InitialCatalog = database });
        using var provider = new SqlServer();
        await Assert.ThrowsAsync<NotSupportedException>(() => provider.GetQueryStoreStateAsync(target));
        var snapshot = await provider.GetMonitoringSnapshotAsync(target,
            new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.QueryStore });
        Assert.Null(snapshot.QueryStore);
        Assert.Contains("unsupported", Assert.Single(snapshot.Errors));
    }

    [Fact]
    public async Task Workload_EmptyDatabaseIsACompleteEmptyObservation()
    {
        await using var fixture = await Fixture.CreateAsync();
        using var provider = new SqlServer();
        var snapshot = await provider.GetIndexUsageAsync(fixture.Target, maximumRows: 1);
        Assert.Empty(snapshot.Indexes);
        Assert.False(snapshot.IsTruncated);
        Assert.Null(snapshot.ServerStartTime);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            provider.GetMonitoringSnapshotAsync(fixture.Target,
                new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.Workload }, cancellation.Token));
        Assert.Equal(cancellation.Token, exception.CancellationToken);
    }

    [Fact]
    public async Task IndexUsage_PermissionFailureIsReportedAsAnUnavailableSection()
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.ExecuteAsync("CREATE USER WorkloadReader WITHOUT LOGIN; CREATE TABLE dbo.Visible(Id int PRIMARY KEY); GRANT SELECT ON dbo.Visible TO WorkloadReader;");
        using var provider = new RestrictedSqlServer();
        var snapshot = await provider.GetMonitoringSnapshotAsync(fixture.Target,
            new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.IndexUsage });
        Assert.Null(snapshot.QueryStore);
        Assert.Null(snapshot.IndexUsage);
        var direct = await Assert.ThrowsAsync<SqlException>(() => provider.GetIndexUsageAsync(fixture.Target));
        Assert.Equal("permission-denied", SqlServer.ClassifySqlMonitoringError(direct));
        Assert.Contains("permission-denied", Assert.Single(snapshot.Errors));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Workload_UnavailableQueryStoreRetainsTheAvailableIndexObservation(bool missingMetadata)
    {
        await using var fixture = await Fixture.CreateAsync();
        await fixture.ExecuteAsync("CREATE TABLE dbo.Visible(Id int PRIMARY KEY)");
        using var provider = new QueryStoreUnavailableSqlServer(missingMetadata);
        var snapshot = await provider.GetMonitoringSnapshotAsync(fixture.Target,
            new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.Workload });
        Assert.Null(snapshot.QueryStore);
        Assert.Contains(missingMetadata ? "metadata-unavailable" : "unsupported", Assert.Single(snapshot.Errors));
        Assert.Single(snapshot.IndexUsage!.Indexes);
        Assert.False(snapshot.IndexUsage.IsTruncated);
    }

    private sealed class QueryStoreUnavailableSqlServer : SqlServer
    {
        private readonly bool _missingMetadata;
        internal QueryStoreUnavailableSqlServer(bool missingMetadata) => _missingMetadata = missingMetadata;

        public override Task<SqlServerQueryStoreState> GetQueryStoreStateAsync(
            SqlServerMonitoringTarget target, CancellationToken cancellationToken = default)
            => _missingMetadata
                ? throw new System.Data.DataException("Query Store metadata is unavailable on this target.")
                : throw new NotSupportedException("Query Store is not available on this target.");
    }

    private sealed class RestrictedSqlServer : SqlServer
    {
        protected override SqlConnection CreateConnection(string connectionString)
            => base.CreateConnection(new SqlConnectionStringBuilder(connectionString) { Pooling = false, Enlist = false }.ConnectionString);

        protected override async Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken)
        {
            await base.OpenConnectionAsync(connection, cancellationToken);
            using var command = connection.CreateCommand();
            command.CommandText = "EXECUTE AS USER = 'WorkloadReader'";
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
    }

    private sealed class Fixture : IAsyncDisposable
    {
        internal string Database { get; } = "DbaxWorkload_" + Guid.NewGuid().ToString("N");
        private string Master { get; }
        private string Connection { get; }
        internal SqlServerMonitoringTarget Target { get; }

        private Fixture(string connection)
        {
            var builder = new SqlConnectionStringBuilder(connection) { InitialCatalog = "master", Pooling = false, Enlist = false };
            Master = builder.ConnectionString;
            builder.InitialCatalog = Database;
            Connection = builder.ConnectionString;
            Target = CreateTarget(builder);
        }

        internal static SqlServerMonitoringTarget CreateTarget(SqlConnectionStringBuilder builder)
            => new()
            {
                ServerOrInstance = builder.DataSource, Database = builder.InitialCatalog, IntegratedSecurity = builder.IntegratedSecurity,
                Username = builder.UserID, Password = builder.Password, TrustServerCertificate = builder.TrustServerCertificate,
                ConnectTimeoutSeconds = builder.ConnectTimeout, ApplicationName = "DbaClientX.WorkloadQualification"
            };

        internal static string ApprovedConnection()
        {
            string? connection = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_WORKLOAD_CONNECTION");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_WORKLOAD_CONNECTION to an instance where unique temporary databases may be created.");
            return connection!;
        }

        internal static async Task<Fixture> CreateAsync(bool enableQueryStore = true)
        {
            var fixture = new Fixture(ApprovedConnection());
            try
            {
                await fixture.ExecuteAsync($"CREATE DATABASE [{fixture.Database}]", master: true);
                await fixture.ExecuteAsync(enableQueryStore
                    ? $"ALTER DATABASE [{fixture.Database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL)"
                    : $"ALTER DATABASE [{fixture.Database}] SET QUERY_STORE = OFF", master: true);
                return fixture;
            }
            catch (Exception primary)
            {
                try { await fixture.DisposeAsync(); }
                catch (Exception cleanup) { throw new AggregateException(primary, cleanup); }
                throw;
            }
        }

        internal async Task ExecuteAsync(string query, bool master = false)
        {
            await using var connection = new SqlConnection(master ? Master : Connection);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = query;
            command.CommandTimeout = 60;
            await command.ExecuteNonQueryAsync();
        }

        public async ValueTask DisposeAsync()
            => await ExecuteAsync($"IF DB_ID(N'{Database}') IS NOT NULL BEGIN ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{Database}]; END", master: true);
    }
}
