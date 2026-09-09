using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_TemporalPeriodColumns_ValidatesProjectionBeforeClear(bool supplied)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} ADD ValidStart datetime2 GENERATED ALWAYS AS ROW START NOT NULL, ValidEnd datetime2 GENERATED ALWAYS AS ROW END NOT NULL, PERIOD FOR SYSTEM_TIME (ValidStart,ValidEnd)");
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Table} (Payload,Enabled,Bytes) VALUES (N'preserved',1,0x01)");
        if (supplied)
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(fixture.LocalPath, "ALTER TABLE SourceRows ADD ValidStart TEXT NOT NULL DEFAULT '2026-01-01'; ALTER TABLE SourceRows ADD ValidEnd TEXT NOT NULL DEFAULT '9999-12-31'");
            await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new() { ClearDestination = true, KeepIdentity = true, CheckpointId = fixture.CopyId }));
            Assert.Equal("preserved", await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT Payload FROM {fixture.Table}"));
            Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
        }
        else
        {
            DbaTableCopyResult result = await fixture.CopyAsync(new() { ClearDestination = true, KeepIdentity = true, CheckpointId = fixture.CopyId });
            Assert.True(result.Verified);
            Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table} WHERE ValidStart < ValidEnd")));
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_ThreePartDestination_ValidatesAutoCreateBeforeClear(bool autoCreate)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        string database = (string)(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, "SELECT DB_NAME()"))!;
        DbaTableCopyDefinition definition = fixture.Definition with { DestinationName = "[" + database.Replace("]", "]]") + "]." + fixture.Table };
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new() { AutoCreateTable = autoCreate });
        Task<DbaTableCopyResult> Copy() => new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { definition }, new() { ClearDestination = true, KeepIdentity = true });
        if (autoCreate) await Assert.ThrowsAsync<ArgumentException>(Copy);
        else Assert.True((await Copy()).Verified);
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_UnwritableSqlDestinationSchema_PreservesExistingRows(bool generated)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        if (generated)
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} DROP COLUMN Bytes; ALTER TABLE {fixture.Table} ADD Bytes AS CONVERT(varbinary(max),'generated')");
        else
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} ADD RequiredValue int NULL");
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"UPDATE {fixture.Table} SET RequiredValue=7; ALTER TABLE {fixture.Table} ALTER COLUMN RequiredValue int NOT NULL");
        }
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new() { ClearDestination = true, KeepIdentity = true, CheckpointId = fixture.CopyId }));
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_UnmappedSqlDefaultOrGeneratedColumn_PreservesSupportedWrites(bool generated)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"ALTER TABLE {fixture.Table} ADD Extra " + (generated ? "AS (7)" : "int NOT NULL DEFAULT 7"));
        DbaTableCopyResult result = await fixture.CopyAsync(new() { KeepIdentity = true, CheckpointId = fixture.CopyId });
        Assert.True(result.Verified);
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table} WHERE Extra=7")));
    }
}
