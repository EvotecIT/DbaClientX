using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Fact]
    public async Task CopyAsync_SnapshotQualifiedSource_UsesSourceDatabaseSetting()
    {
        string? other = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_OTHER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(other), "Requires isolated SQL Server database with snapshot isolation disabled.");
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        string database = new SqlConnectionStringBuilder(fixture.Connection).InitialCatalog.Replace("]", "]]");
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.LocalPath, "CREATE TABLE SnapshotRows (Id INTEGER PRIMARY KEY, Payload TEXT, Optional TEXT, Enabled INTEGER, Bytes BLOB)");
        var definition = new DbaTableCopyDefinition($"[{database}].{fixture.Table}", "SnapshotRows", new[] { "Id" }) { UseKeysetPagination = true };
        var source = new SqlServerTableCopyAdapter(other!) { ReadConsistency = DbaTableCopyReadConsistency.Snapshot };
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(source, fixture.Source, new[] { definition }, new() { VerifyContent = true, PageSize = 1, MaxPageBytes = 4096 });
        Assert.True(result.Verified);
        Assert.Equal(3L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.LocalPath, "SELECT COUNT(*) FROM SnapshotRows")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_SnapshotDisabledQualifiedDatabase_RejectsBeforeReadingAnyTable(bool includeFirstTable)
    {
        string? other = Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_OTHER_TEST_CONNECTION");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(other), "Requires isolated SQL Server database with snapshot isolation disabled.");
        using Fixture fixture = await Fixture.CreateAsync();
        using Fixture otherFixture = await Fixture.CreateAsync(other);
        await fixture.CopyAsync(new() { KeepIdentity = true });
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.LocalPath, "CREATE TABLE OtherRows (Id INTEGER PRIMARY KEY, Payload TEXT, Optional TEXT, Enabled INTEGER, Bytes BLOB)");
        string database = new SqlConnectionStringBuilder(other!).InitialCatalog;
        var definitions = new List<DbaTableCopyDefinition>();
        if (includeFirstTable) definitions.Add(new(fixture.Table, "SourceRows", new[] { "Id" }) { UseKeysetPagination = true });
        definitions.Add(new($"[{database.Replace("]", "]]")}].{otherFixture.Table}", "OtherRows", new[] { "Id" }) { UseKeysetPagination = true });
        var source = new SqlServerTableCopyAdapter(fixture.Connection) { ReadConsistency = DbaTableCopyReadConsistency.Snapshot };
        int progress = 0;
        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(source, fixture.Source, definitions, new() { VerifyContent = true, ClearDestination = true, Progress = _ => progress++ }));
        Assert.Contains(database, error.Message);
        Assert.Contains("ALLOW_SNAPSHOT_ISOLATION", error.Message);
        Assert.Equal(0, progress);
        Assert.Equal(3L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.LocalPath, "SELECT COUNT(*) FROM SourceRows")));
    }
}
