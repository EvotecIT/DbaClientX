using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopyReliabilityTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public async Task CopyAsync_MixedSqliteStorageValues_PreservesEveryOriginalValue(int pageSize)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        const string schema = "CREATE TABLE MixedRows (Id INTEGER PRIMARY KEY, Payload NUMERIC)";
        sqlite.ExecuteNonQuery(fixture.SourcePath, schema + "; INSERT INTO MixedRows VALUES (1,1),(2,1.5),(3,'word')");
        sqlite.ExecuteNonQuery(fixture.DestinationPath, schema);
        var definition = new DbaTableCopyDefinition("MixedRows", "MixedRows", new[] { "Id" }) { UseKeysetPagination = true };
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { definition }, new() { VerifyContent = true, PageSize = pageSize, MaxPageBytes = 4096 });
        Assert.True(result.Verified);
        Assert.Equal(1.5d, sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM MixedRows WHERE Id=2"));
        Assert.Equal("real", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT typeof(Payload) FROM MixedRows WHERE Id=2"));
        Assert.Equal("word", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM MixedRows WHERE Id=3"));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task CopyAsync_MissingSqliteDestinationColumn_PreservesDestinationBeforeClear(int mode)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "ALTER TABLE DestinationRows RENAME COLUMN Payload TO Other; INSERT INTO DestinationRows VALUES ('keep',1,'original')");
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.CopyAsync(new() { ClearDestination = true, VerifyContent = mode != 0, CheckpointId = mode == 2 ? "schema" : null }));
        Assert.Equal("original", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Other FROM DestinationRows"));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }

    [Fact]
    public async Task CopyAsync_EmptyVerifiedProjection_PreservesDestination()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "INSERT INTO DestinationRows VALUES ('keep',1,'original')");
        DbaTableCopyDefinition definition = fixture.Definition with
        {
            ExcludedColumns = new[] { "GroupName", "Number", "Payload" },
            DestinationOrderByColumns = new[] { "GroupName", "Number" }
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { definition }, new() { ClearDestination = true, CheckpointId = "empty-projection" }));
        Assert.Equal("original", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM DestinationRows"));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(definition));
    }

    [Fact]
    public async Task CopyAsync_ReorderedExclusions_ResumesSameProjection()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.SourcePath, "ALTER TABLE SourceRows ADD COLUMN OmitA TEXT; ALTER TABLE SourceRows ADD COLUMN OmitB TEXT");
        DbaTableCopyDefinition definition = fixture.Definition with { ExcludedColumns = new[] { "OmitA", "OmitB" } };
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { definition }, new()
            {
                CheckpointId = "exclusions", PageSize = 3,
                Progress = value => { if (value.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
            }, cancellation.Token));
        DbaTableCopyResult resumed = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { definition with { ExcludedColumns = new[] { "OmitB", "OmitA" } } }, new() { CheckpointId = "exclusions", Resume = true });
        Assert.True(resumed.Verified);
        Assert.Equal(9, resumed.CopiedRows);
    }

    [Fact]
    public async Task ReadPageAsync_RealSQLiteKeys_RoundTripsContinuation()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.SourcePath, "CREATE TABLE RealKeys (Value REAL NOT NULL PRIMARY KEY); INSERT INTO RealKeys VALUES (-1e200),(0.125),(1e200)");
        var definition = new DbaTableCopyDefinition("RealKeys", "RealKeys", new[] { "Value" }) { UseKeysetPagination = true };
        var actual = new List<double>();
        string? token = null;
        do
        {
            using DbaTableCopyPage page = await fixture.Source.ReadPageAsync(new(definition, token, 1));
            actual.AddRange(page.Data.Rows.Cast<DataRow>().Select(row => (double)row[0]));
            token = page.ContinuationToken;
            Assert.True(actual.Count <= 3);
        } while (token != null);
        Assert.Equal(new[] { -1e200, 0.125, 1e200 }, actual);
    }
}
