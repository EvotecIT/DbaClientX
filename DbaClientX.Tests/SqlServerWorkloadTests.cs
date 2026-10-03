using System.Data;
using DBAClientX;
using DBAClientX.SqlServerMonitoring;

namespace DbaClientX.Tests;

public sealed class SqlServerWorkloadTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(10001)]
    [InlineData(int.MaxValue)]
    public async Task IndexUsage_InvalidBoundIsRejectedBeforeConnecting(int maximumRows)
    {
        using var provider = new SqlServer();
        var target = new SqlServerMonitoringTarget { ServerOrInstance = "unavailable", ConnectTimeoutSeconds = 1 };
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => provider.GetIndexUsageAsync(target, maximumRows));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => provider.GetMonitoringSnapshotAsync(target,
            new SqlServerMonitoringOptions { Scope = SqlServerMonitoringScope.IndexUsage, MaximumIndexUsageRows = maximumRows }));
    }

    [Fact]
    public void Mapping_PreservesAbsentCountersStatisticsAndServerLocalTimes()
    {
        using var table = IndexTable();
        var time = new DateTime(2026, 10, 3, 9, 15, 0, DateTimeKind.Utc);
        table.Rows.Add("dbo", "Events", "UX_Events_Id", 123, 2, false, true, true, false,
            DBNull.Value, 0L, 7L, 9L, DBNull.Value, time, DBNull.Value, time,
            DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, time);
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        var (index, start) = SqlServerWorkloadMappers.MapIndexUsage(reader);
        Assert.Null(index.UserSeeks);
        Assert.Equal(0L, index.UserScans);
        Assert.Equal(7L, index.UserLookups);
        Assert.Equal(9L, index.UserUpdates);
        Assert.Null(index.StatisticsRows);
        Assert.Null(index.StatisticsLastUpdated);
        Assert.True(index.IsUniqueConstraint);
        Assert.Equal(time.Ticks, index.LastUserScan!.Value.Ticks);
        Assert.Equal(DateTimeKind.Unspecified, index.LastUserScan.Value.Kind);
        Assert.Equal(DateTimeKind.Unspecified, start!.Value.Kind);
    }

    [Fact]
    public void QueryStoreMapping_PreservesStateMismatchAndCombinedReadOnlyReasons()
    {
        using var table = new DataTable();
        foreach (string name in new[] { "DatabaseName", "DesiredState", "ActualState", "CaptureMode" })
            table.Columns.Add(name, typeof(string));
        foreach (string name in new[] { "CurrentStorageSizeMb", "MaximumStorageSizeMb", "ReadOnlyReason" })
            table.Columns.Add(name, typeof(long));
        table.Columns.Add("DatabaseId", typeof(int));
        table.Rows.Add("App", "READ_WRITE", "READ_ONLY", "AUTO", 900L, 1000L, 65544L, 5);
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        var state = SqlServerWorkloadMappers.MapQueryStoreState(reader);
        Assert.Equal("READ_WRITE", state.DesiredState);
        Assert.Equal("READ_ONLY", state.ActualState);
        Assert.Equal(65544L, state.ReadOnlyReason);
        Assert.Equal(900L, state.CurrentStorageSizeMb);
        Assert.Equal(1000L, state.MaximumStorageSizeMb);
    }

    [Fact]
    public void QueryStoreMapping_UnavailableMetadataDoesNotBecomeAnOffObservation()
    {
        using var table = new DataTable();
        table.Columns.Add("DatabaseId", typeof(int));
        table.Columns.Add("DesiredState", typeof(string));
        table.Rows.Add(5, DBNull.Value);
        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        var exception = Assert.Throws<DataException>(() => SqlServerWorkloadMappers.MapQueryStoreState(reader));
        Assert.Equal("metadata-unavailable", SqlServer.ClassifySqlMonitoringError(exception));
    }

    [Theory]
    [InlineData(229)]
    [InlineData(297)]
    [InlineData(300)]
    [InlineData(15562)]
    public void Monitoring_ReportsPermissionFailureFromRedactedProviderCode(int code)
    {
        var exception = new DbaQueryExecutionException("The database operation failed.", null,
            new InvalidOperationException("private provider details"), code);
        Assert.Equal("permission-denied", SqlServer.ClassifySqlMonitoringError(exception));
    }

    private static DataTable IndexTable()
    {
        var table = new DataTable();
        foreach (string name in new[] { "SchemaName", "TableName", "IndexName" })
            table.Columns.Add(name, typeof(string));
        foreach (string name in new[] { "ObjectId", "IndexId" })
            table.Columns.Add(name, typeof(int));
        foreach (string name in new[] { "IsPrimaryKey", "IsUnique", "IsUniqueConstraint", "IsDisabled" })
            table.Columns.Add(name, typeof(bool));
        foreach (string name in new[] { "UserSeeks", "UserScans", "UserLookups", "UserUpdates" })
            table.Columns.Add(name, typeof(long));
        foreach (string name in new[] { "LastUserSeek", "LastUserScan", "LastUserLookup", "LastUserUpdate", "StatisticsLastUpdated" })
            table.Columns.Add(name, typeof(DateTime));
        foreach (string name in new[] { "StatisticsRows", "StatisticsRowsSampled", "StatisticsModificationCounter" })
            table.Columns.Add(name, typeof(long));
        table.Columns.Add("ServerStartTime", typeof(DateTime));
        return table;
    }
}
