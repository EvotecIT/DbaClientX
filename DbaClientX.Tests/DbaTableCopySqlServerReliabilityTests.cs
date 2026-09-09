using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopySqlServerReliabilityTests
{
    [Fact]
    public async Task ReadPageAsync_ByteLimitedSqlPage_StopsTransferAndPreservesSnapshot()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection,
            $"INSERT INTO {fixture.Table} (Payload, [Optional], Enabled, Bytes) " +
            "SELECT N'original',NULL,1,CONVERT(varbinary(max),REPLICATE(CAST('x' AS varchar(max)),524288)) " +
            "FROM (VALUES(1),(2),(3),(4),(5),(6),(7),(8)) a(n) CROSS JOIN (VALUES(1),(2),(3),(4)) b(n)");
        SqlConnection? connection = null;
        var source = new SqlServerTableCopyAdapter(fixture.Connection, connectionOptions: new SqlServerConnectionOptions
        {
            ConnectionFactory = value => connection = new SqlConnection(value) { StatisticsEnabled = true }
        }) { ReadConsistency = DbaTableCopyReadConsistency.Snapshot };
        var definition = new DbaTableCopyDefinition(fixture.Table, fixture.Table, new[] { "Id" }) { UseKeysetPagination = true };
        using (await source.OpenReadSessionAsync())
        {
            Assert.Equal(32, await source.CountRowsAsync(definition));
            connection!.ResetStatistics();
            using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, null, 32) { MaxBytes = 600_000 });
            Assert.Single(page.Data.Rows.Cast<DataRow>());
            long transferred = Convert.ToInt64(connection.RetrieveStatistics()["BytesReceived"]);
            Assert.True(transferred < 3 * 1024 * 1024, $"A one-row page transferred {transferred} bytes instead of canceling the unused result.");
            Assert.Equal(32, await source.CountRowsAsync(definition));
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"UPDATE {fixture.Table} SET Payload=N'changed'");
            using DbaTableCopyPage next = await source.ReadPageAsync(new(definition, page.ContinuationToken, 32) { MaxBytes = 600_000 });
            Assert.Equal("original", next.Data.Rows[0]["Payload"]);
            Assert.True(Convert.ToInt64(next.Data.Rows[0]["Id"]) > Convert.ToInt64(page.Data.Rows[0]["Id"]));
        }
        using DbaTableCopyPage latest = await source.ReadPageAsync(new(definition, null, 1));
        Assert.Equal("changed", latest.Data.Rows[0]["Payload"]);
    }

    [Fact]
    public async Task CopyAsync_SqlServerInterruption_PreservesIdentityNullsAndAtomicCheckpoints()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.CopyAsync(new()
        {
            KeepIdentity = true, CheckpointId = fixture.CopyId, PageSize = 1,
            Progress = progress => { if (progress.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
        }, cancellation.Token));
        DbaTableCopyCheckpoint checkpoint = (await fixture.Destination.ReadCheckpointAsync(fixture.Definition))!;
        Assert.Equal(1, checkpoint.CopiedRows);
        using var page = new DataTable();
        page.Columns.Add("Id", typeof(long));
        page.Columns.Add("Payload", typeof(string));
        page.Columns.Add("Optional", typeof(string));
        page.Columns.Add("Enabled", typeof(bool));
        page.Columns.Add("Bytes", typeof(byte[]));
        page.Rows.Add(1006L, "rolled back", DBNull.Value, true, new byte[] { 1 });
        page.Rows.Add(41L, "duplicate", DBNull.Value, false, new byte[] { 2 });
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Destination.CommitPageAsync(fixture.Definition, page,
            new() { KeepIdentity = true, BatchSize = 1, CheckpointId = fixture.CopyId }, checkpoint,
            checkpoint with { CopiedRows = 3 }));
        Assert.Equal(1L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table}")));
        Assert.Equal(checkpoint, await fixture.Destination.ReadCheckpointAsync(fixture.Definition));

        DbaTableCopyResult result = await fixture.CopyAsync(new() { KeepIdentity = true, CheckpointId = fixture.CopyId, Resume = true, PageSize = 2 });
        Assert.True(result.Verified);
        Assert.Equal(1, Assert.Single(result.Tables).ResumedRows);
        Assert.Equal(3L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT COUNT_BIG(*) FROM {fixture.Table} WHERE [Optional] IS NULL")));
        Assert.Equal(1005L, Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT MAX(Id) FROM {fixture.Table}")));

        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.LocalPath, "CREATE TABLE RoundTripRows (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Optional TEXT NULL, Enabled INTEGER NOT NULL, Bytes BLOB NOT NULL)");
        var reverseDefinition = new DbaTableCopyDefinition(fixture.Table, "RoundTripRows", new[] { "Id" }) { UseKeysetPagination = true };
        DbaTableCopyResult reverse = await new DbaTableCopyEngine().CopyAsync(fixture.Destination, new SQLiteTableCopyAdapter(fixture.LocalPath),
            new[] { reverseDefinition }, new() { VerifyContent = true, PageSize = 1 });
        Assert.True(reverse.Verified);
        Assert.Equal("Zażółć 😀 日本語", sqlite.ExecuteScalar(fixture.LocalPath, "SELECT Payload FROM RoundTripRows WHERE Id=99"));
        await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"INSERT INTO {fixture.Table} (Payload, Enabled, Bytes) VALUES (N'new write', 1, 0x01)");
        Assert.True(Convert.ToInt64(await fixture.Sql.ExecuteScalarAsync(fixture.Connection, $"SELECT MAX(Id) FROM {fixture.Table}")) > 1005);
    }

    [Fact]
    public async Task ReadSession_Snapshot_SeesStableRowsAcrossConcurrentChanges()
    {
        using Fixture fixture = await Fixture.CreateAsync();
        await fixture.CopyAsync(new() { VerifyContent = true, KeepIdentity = true });
        var source = new SqlServerTableCopyAdapter(new DbaProviderTableCopyAdapterOptions
        {
            Provider = DbaTableCopyProvider.SqlServer, ConnectionString = fixture.Connection,
            ReadConsistency = DbaTableCopyReadConsistency.Snapshot
        });
        var definition = new DbaTableCopyDefinition(fixture.Table, fixture.Table, new[] { "Id" }) { UseKeysetPagination = true };
        using (await source.OpenReadSessionAsync())
        {
            Assert.Equal(3, await source.CountRowsAsync(definition));
            await fixture.Sql.ExecuteNonQueryAsync(fixture.Connection, $"UPDATE {fixture.Table} SET Payload=N'changed' WHERE Id=41");
            using DbaTableCopyPage page = await source.ReadPageAsync(new(definition, null, 1));
            Assert.Equal("Zażółć 😀 日本語", page.Data.Rows[0]["Payload"]);
        }
        using DbaTableCopyPage current = await source.ReadPageAsync(new(definition, null, 1));
        Assert.Equal("changed", current.Data.Rows[0]["Payload"]);
    }

    private sealed class Fixture : IDisposable
    {
        internal SqlServer Sql { get; } = new();
        internal string Connection { get; }
        internal string CopyId { get; } = "dbax-contract-" + Guid.NewGuid().ToString("N");
        internal string Table { get; } = "dbo.DbaxMigration" + Guid.NewGuid().ToString("N");
        internal string LocalPath { get; } = Path.Combine(Path.GetTempPath(), "dbax-sql-migration-" + Guid.NewGuid().ToString("N") + ".sqlite");
        internal SQLiteTableCopyAdapter Source { get; }
        internal SqlServerTableCopyAdapter Destination { get; }
        internal DbaTableCopyDefinition Definition { get; }

        private Fixture(string connection)
        {
            Connection = connection;
            Source = new SQLiteTableCopyAdapter(LocalPath);
            Destination = new SqlServerTableCopyAdapter(connection);
            Definition = new("SourceRows", Table, new[] { "Id" }, ColumnTypeConversions: new Dictionary<string, DbaTableCopyColumnType> { ["Enabled"] = DbaTableCopyColumnType.Boolean }) { UseKeysetPagination = true };
        }

        internal static async Task<Fixture> CreateAsync(string? connectionOverride = null)
        {
            string? connection = connectionOverride ?? Environment.GetEnvironmentVariable("DBACLIENTX_SQLSERVER_TEST_CONNECTION");
            Assert.SkipWhen(string.IsNullOrWhiteSpace(connection), "Set DBACLIENTX_SQLSERVER_TEST_CONNECTION to an isolated SQL Server database with ALLOW_SNAPSHOT_ISOLATION enabled.");
            var fixture = new Fixture(connection!);
            try
            {
                await fixture.Sql.ExecuteNonQueryAsync(connection!, $"CREATE TABLE {fixture.Table} (Id bigint IDENTITY PRIMARY KEY, Payload nvarchar(max) NOT NULL, [Optional] nvarchar(50) NULL DEFAULT N'default value', Enabled bit NOT NULL, Bytes varbinary(max) NOT NULL)");
                using var sqlite = new SQLite();
                sqlite.ExecuteNonQuery(fixture.LocalPath, "CREATE TABLE SourceRows (Id INTEGER PRIMARY KEY, Payload TEXT NOT NULL, Optional TEXT NULL, Enabled INTEGER NOT NULL, Bytes BLOB NOT NULL)");
                foreach (long id in new[] { 41L, 99L, 1005L })
                    sqlite.ExecuteNonQuery(fixture.LocalPath, "INSERT INTO SourceRows VALUES (@id,@payload,NULL,1,@bytes)",
                        new Dictionary<string, object?> { ["@id"] = id, ["@payload"] = "Zażółć 😀 日本語", ["@bytes"] = new byte[] { 0, 1, 128, 255 } });
                return fixture;
            }
            catch { fixture.Dispose(); throw; }
        }

        internal Task<DbaTableCopyResult> CopyAsync(DbaTableCopyOptions options, CancellationToken cancellationToken = default)
            => new DbaTableCopyEngine().CopyAsync(Source, Destination, new[] { Definition }, options, cancellationToken);

        public void Dispose()
        {
            try
            {
                Sql.ExecuteNonQuery(Connection, $"DROP TABLE IF EXISTS {Table}; IF OBJECT_ID(N'dbo.DbaClientX_TableCopyCheckpoints', N'U') IS NOT NULL DELETE FROM dbo.DbaClientX_TableCopyCheckpoints WHERE CopyId=@copyId",
                    new Dictionary<string, object?> { ["@copyId"] = CopyId });
            }
            finally
            {
                Sql.Dispose();
                File.Delete(LocalPath);
                File.Delete(LocalPath + "-wal");
                File.Delete(LocalPath + "-shm");
            }
        }
    }
}
