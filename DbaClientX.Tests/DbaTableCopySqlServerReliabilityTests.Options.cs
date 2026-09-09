using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Fact]
    public async Task CopyAsync_InternalBulkTransactionOption_UsesAtomicCheckpointTransaction()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { KeepIdentity = true });
        var destination = new SqlServerTableCopyAdapter(fixture.Connection,
            bulkInsertOptions: new() { BulkCopyOptions = SqlBulkCopyOptions.UseInternalTransaction });
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { fixture.Definition },
            new() { KeepIdentity = true, CheckpointId = fixture.CopyId, ClearDestination = true, PageSize = 1 });
        Assert.True(result.Verified);
        Assert.Equal(3, result.CopiedRows);
        Assert.True((await destination.ReadCheckpointAsync(fixture.Definition))!.Completed);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_AdapterMappings_RefusesBeforeClearOrResume(bool resume)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.CopyAsync(new()
        {
            KeepIdentity = true, CheckpointId = fixture.CopyId, PageSize = 1,
            Progress = value => { if (value.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
        }, cancellation.Token));
        DbaTableCopyCheckpoint before = (await fixture.Destination.ReadCheckpointAsync(fixture.Definition))!;
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new()
        {
            ColumnMappings = new Dictionary<string, string> { ["Payload"] = "Optional", ["Optional"] = "Payload" }
        });
        await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, destination,
            new[] { fixture.Definition }, new()
            {
                KeepIdentity = true, CheckpointId = resume ? fixture.CopyId : fixture.CopyId + "-new",
                ClearDestination = !resume, Resume = resume
            }));
        Assert.Equal(before, await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
        Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task CopyAsync_RequireEmptyAutoCreatedDestination_CreatesMissingTable(bool emptySource, bool tolerateMissing)
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE {fixture.Table}");
        if (emptySource)
        {
            using var sqlite = new SQLite();
            sqlite.ExecuteNonQuery(fixture.LocalPath, "DELETE FROM SourceRows");
        }
        var destination = new SqlServerTableCopyAdapter(fixture.Connection, bulkInsertOptions: new() { AutoCreateTable = true }, treatMissingTablesAsEmpty: tolerateMissing);
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, destination, new[] { fixture.Definition },
            new() { RequireEmptyDestination = true });
        Assert.True(result.Verified);
        Assert.Equal(emptySource ? 0 : 3, result.CopiedRows);
        Assert.Equal(emptySource ? 0L : 3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
    }

    [Fact]
    public async Task ReadPageAsync_BooleanCompositeKey_RoundTripsContinuation()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        string table = "dbo.DbaxBool" + Guid.NewGuid().ToString("N");
        try
        {
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"CREATE TABLE {table} (Enabled bit NOT NULL, Id bigint NOT NULL, PRIMARY KEY(Enabled,Id)); INSERT INTO {table} VALUES (0,1),(0,2),(1,1)");
            var source = new SqlServerTableCopyAdapter(fixture.Connection);
            var definition = new DbaTableCopyDefinition(table, table, new[] { "Enabled", "Id" }) { UseKeysetPagination = true };
            var values = new List<string>();
            string? token = null;
            do
            {
                using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, token, 1));
                values.AddRange(page.Data.Rows.Cast<DataRow>().Select(row => $"{row[0]}:{row[1]}"));
                token = page.ContinuationToken;
                Assert.True(values.Count <= 3);
            } while (token != null);
            Assert.Equal(new[] { "False:1", "False:2", "True:1" }, values);
        }
        finally { await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"DROP TABLE IF EXISTS {table}"); }
    }
}
