using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Fact]
    public async Task CopyAsync_OrdinaryBulkMappings_PreservesSupportedMappedWrites()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        using (var sqlite = new SQLite()) sqlite.ExecuteNonQuery(fixture.LocalPath, "UPDATE SourceRows SET Optional='redirected'");
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new()
        {
            ColumnMappings = new Dictionary<string, string> { ["Payload"] = "Optional", ["Optional"] = "Payload" }
        });
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { fixture.Definition }, new() { ClearDestination = true, KeepIdentity = true });
        Assert.True(result.Verified);
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table} WHERE Payload='redirected' AND [Optional]=N'Zażółć 😀 日本語'")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_InvalidBulkNotification_PreservesDestinationBeforeClear(bool checkpoint)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new() { NotifyAfter = 0 });
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { fixture.Definition },
            new() { ClearDestination = true, KeepIdentity = true, CheckpointId = checkpoint ? fixture.CopyId : null }));
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }

    [Fact]
    public async Task CopyAsync_InvalidOrdinaryBulkMapping_PreservesDestinationBeforeClear()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new()
        {
            ColumnMappings = new Dictionary<string, string> { ["MissingSourceColumn"] = "Payload" }
        });
        await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { fixture.Definition },
            new() { ClearDestination = true, KeepIdentity = true }));
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
    }

    [Theory]
    [InlineData(SqlBulkCopyOptions.KeepIdentity)]
    [InlineData(SqlBulkCopyOptions.FireTriggers)]
    [InlineData(SqlBulkCopyOptions.AllowEncryptedValueModifications)]
    public async Task CopyAsync_UnboundBulkWriteSemantics_PreservesDestinationBeforeCheckpoint(SqlBulkCopyOptions flags)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new() { BulkCopyOptions = flags });
        await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { fixture.Definition },
            new() { ClearDestination = true, CheckpointId = fixture.CopyId }));
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task CopyAsync_MissingSqlDestinationColumn_PreservesDestinationBeforeClear(int mode)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} DROP COLUMN Bytes");
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.CopyAsync(new()
        {
            ClearDestination = true, KeepIdentity = true, VerifyContent = mode != 0, CheckpointId = mode == 2 ? fixture.CopyId : null
        }));
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }
}
