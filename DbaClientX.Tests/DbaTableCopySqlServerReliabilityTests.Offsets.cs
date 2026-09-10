using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_DestinationOffsetChangesAtSameInstant_DetectsChangedContent(bool resume)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string sourceName = "dbo.DbaxOffsetSource" + Guid.NewGuid().ToString("N");
        string targetName = "dbo.DbaxOffsetTarget" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {sourceName} (Id int PRIMARY KEY, Value datetimeoffset(7) NOT NULL); INSERT INTO {sourceName} VALUES (1,'2026-01-01T12:00:00.0000001+00:00'); SELECT TOP(0) * INTO {targetName} FROM {sourceName}");
            var definition = new DbaTableCopyDefinition(sourceName, targetName, new[] { "Id" }) { UseKeysetPagination = true };
            var source = new SqlServerTableCopyAdapter(fixture.Connection);
            var destination = new SqlServerTableCopyAdapter(fixture.Connection);
            void ChangeOffset() => fixture.Sql.ExecuteNonQuery(fixture.Connection, $"UPDATE {targetName} SET Value=SWITCHOFFSET(Value,'+02:00')");
            DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }, new()
            {
                VerifyContent = true, CheckpointId = fixture.CopyId,
                Progress = progress => { if (!resume && progress.Phase == DbaTableCopyPhase.Copy && progress.RowsCopied == 1) ChangeOffset(); }
            });
            if (resume)
            {
                Assert.True(result.Verified);
                ChangeOffset();
                await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(source, destination, new[] { definition }, new() { CheckpointId = fixture.CopyId, Resume = true }));
            }
            else
            {
                Assert.False(result.Verified);
                Assert.NotEqual(Assert.Single(result.Tables).SourceContentHash, Assert.Single(result.Tables).DestinationContentHash);
            }
            Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {targetName}")));
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {sourceName}; DROP TABLE IF EXISTS {targetName}"); }
    }
}
