using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed class DbaTableCopyReliabilityTests
{
    [Fact]
    public async Task CopyAsync_DifferentGeneratedDestinationKey_VerifiesOnlyCopiedColumns()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath,
            "DROP TABLE DestinationRows; CREATE TABLE DestinationRows (VerificationId INTEGER PRIMARY KEY AUTOINCREMENT, GroupName TEXT NOT NULL, Number INTEGER NOT NULL, Payload TEXT NULL)");
        DbaTableCopyDefinition definition = fixture.Definition with { DestinationOrderByColumns = new[] { "VerificationId" } };
        DbaTableCopyResult result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination, new[] { definition },
            new DbaTableCopyOptions { VerifyContent = true, CheckpointId = "generated-key", PageSize = 1 });
        Assert.True(result.Verified);
        Assert.Equal(12, Assert.Single(result.Tables).CopiedRows);
        Assert.Equal(12L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT COUNT(DISTINCT VerificationId) FROM DestinationRows")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { definition with { DestinationOrderByColumns = new[] { "GroupName", "Number" } } },
            new DbaTableCopyOptions { CheckpointId = "generated-key", Resume = true }));
        Assert.Equal(12, fixture.DestinationCount());
    }

    [Fact]
    public async Task CopyAsync_ResumeBeforeFirstCheckpoint_RequiresEmptyDestination()
    {
        using var fixture = new Fixture();
        DbaTableCopyResult result = await fixture.CopyAsync(new DbaTableCopyOptions { CheckpointId = "preflight", Resume = true, PageSize = 3 });
        Assert.True(result.Verified);
        Assert.Equal(0, Assert.Single(result.Tables).ResumedRows);
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new DbaTableCopyOptions { CheckpointId = "different", Resume = true }));
        Assert.Equal(12, fixture.DestinationCount());
    }

    [Fact]
    public async Task CopyAsync_BoundedCompositeKeyPages_VerifiesAllPayloads()
    {
        using var fixture = new Fixture();
        DbaTableCopyResult result = await fixture.CopyAsync(new DbaTableCopyOptions { VerifyContent = true, PageSize = 100, MaxPageBytes = 1200, CheckpointId = "bounded" });
        Assert.True(result.Verified);
        DbaTableCopyTableResult table = Assert.Single(result.Tables);
        Assert.Equal(12, table.CopiedRows);
        Assert.True(table.PageCount > 1);
        Assert.Equal(table.SourceContentHash, table.DestinationContentHash);
        Assert.True((await fixture.Destination.ReadCheckpointAsync(fixture.Definition))!.Completed);
    }

    [Fact]
    public async Task CopyAsync_CancelAfterCommittedPage_ResumesWithoutDuplicates()
    {
        using var fixture = new Fixture();
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.CopyAsync(new DbaTableCopyOptions
        {
            CheckpointId = "resume", PageSize = 3,
            Progress = progress => { if (progress.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
        }, cancellation.Token));
        DbaTableCopyCheckpoint? partial = await fixture.Destination.ReadCheckpointAsync(fixture.Definition);
        Assert.NotNull(partial);
        Assert.Equal(3, partial.CopiedRows);
        Assert.False(partial.Completed);
        Assert.Equal(3L, fixture.DestinationCount());
        DbaTableCopyResult resumed = await fixture.CopyAsync(new DbaTableCopyOptions { CheckpointId = "resume", Resume = true, PageSize = 2 });
        Assert.True(resumed.Verified);
        Assert.Equal(3, Assert.Single(resumed.Tables).ResumedRows);
        Assert.Equal(12L, fixture.DestinationCount());
        DbaTableCopyResult completedResume = await fixture.CopyAsync(new DbaTableCopyOptions { CheckpointId = "resume", Resume = true });
        Assert.True(completedResume.Verified);
        Assert.Equal(12, Assert.Single(completedResume.Tables).ResumedRows);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CopyAsync_ChangedSourceOrDestination_RefusesResumeWithoutWriting(bool changeSource)
    {
        using var fixture = new Fixture();
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.CopyAsync(new DbaTableCopyOptions
        {
            CheckpointId = "changed", PageSize = 3,
            Progress = progress => { if (progress.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
        }, cancellation.Token));
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(changeSource ? fixture.SourcePath : fixture.DestinationPath,
            $"UPDATE {(changeSource ? "SourceRows" : "DestinationRows")} SET Payload='changed' WHERE Number=1");
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new DbaTableCopyOptions { CheckpointId = "changed", Resume = true }));
        Assert.Equal(3L, fixture.DestinationCount());
        Assert.Equal(3, (await fixture.Destination.ReadCheckpointAsync(fixture.Definition))!.CopiedRows);
    }

    [Fact]
    public async Task CommitPageAsync_FailingRow_RollsBackRowsAndCheckpoint()
    {
        using var fixture = new Fixture();
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.CopyAsync(new DbaTableCopyOptions
        {
            CheckpointId = "atomic", PageSize = 3,
            Progress = progress => { if (progress.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
        }, cancellation.Token));
        DbaTableCopyCheckpoint checkpoint = (await fixture.Destination.ReadCheckpointAsync(fixture.Definition))!;
        using var page = new DataTable();
        page.Columns.Add("GroupName", typeof(string));
        page.Columns.Add("Number", typeof(long));
        page.Columns.Add("Payload", typeof(string));
        page.Rows.Add("new", 100L, "new row");
        page.Rows.Add("A", 1L, "duplicate key");
        await Assert.ThrowsAnyAsync<Exception>(() => fixture.Destination.CommitPageAsync(fixture.Definition, page,
            new DbaTableCopyOptions { BatchSize = 1 }, checkpoint, checkpoint with { CopiedRows = 5 }));
        Assert.Equal(3L, fixture.DestinationCount());
        Assert.Equal(checkpoint, await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }

    [Fact]
    public async Task ReadPageAsync_CompositeKey_HandlesQuotesUnicodeAndSparseIntegers()
    {
        using var fixture = new Fixture();
        string? token = null;
        var keys = new List<string>();
        do
        {
            using DbaTableCopyPage page = await fixture.Source.ReadPageAsync(new DbaTableCopyPageRequest(fixture.Definition, token, 2));
            foreach (DataRow row in page.Data.Rows) keys.Add($"{row["GroupName"]}/{row["Number"]}");
            token = page.ContinuationToken;
        } while (token != null);
        Assert.Equal(12, keys.Count);
        Assert.Equal(12, keys.Distinct(StringComparer.Ordinal).Count());
        Assert.Contains("日本語'quoted/100", keys);
    }

    [Fact]
    public async Task CopyAsync_OversizedRow_FailsBeforeClearingDestination()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "INSERT INTO DestinationRows VALUES ('existing',1,'retain')");
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new DbaTableCopyOptions
        {
            VerifyContent = true, ClearDestination = true, MaxPageBytes = 100
        }));
        Assert.Equal("retain", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM DestinationRows"));
    }

    [Fact]
    public async Task CopyAsync_DuplicatePagingKey_FailsBeforeClearingDestination()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "INSERT INTO DestinationRows VALUES ('existing',1,'retain')");
        DbaTableCopyDefinition invalid = fixture.Definition with { OrderByColumns = new[] { "GroupName" } };
        await Assert.ThrowsAsync<InvalidOperationException>(() => new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { invalid }, new DbaTableCopyOptions { VerifyContent = true, ClearDestination = true, PageSize = 1 }));
        Assert.Equal("retain", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM DestinationRows"));
    }

    private sealed class Fixture : IDisposable
    {
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "dbax-copy-reliability-" + Guid.NewGuid().ToString("N"));
        internal string SourcePath => Path.Combine(_directory, "source.sqlite");
        internal string DestinationPath => Path.Combine(_directory, "destination.sqlite");
        internal SQLiteTableCopyAdapter Source { get; }
        internal SQLiteTableCopyAdapter Destination { get; }
        internal DbaTableCopyDefinition Definition { get; } = new("SourceRows", "DestinationRows", new[] { "GroupName", "Number" }) { UseKeysetPagination = true };

        internal Fixture()
        {
            Directory.CreateDirectory(_directory);
            using var sqlite = new SQLite();
            const string columns = "(GroupName TEXT NOT NULL, Number INTEGER NOT NULL, Payload TEXT NULL, PRIMARY KEY (GroupName,Number))";
            sqlite.ExecuteNonQuery(SourcePath, "CREATE TABLE SourceRows " + columns);
            sqlite.ExecuteNonQuery(DestinationPath, "CREATE TABLE DestinationRows " + columns);
            foreach (string group in new[] { "a", "A", "日本語'quoted" })
                foreach (long number in new[] { -5L, 1L, 9L, 100L })
                    sqlite.ExecuteNonQuery(SourcePath, "INSERT INTO SourceRows VALUES (@group,@number,@payload)", new Dictionary<string, object?>
                    {
                        ["@group"] = group, ["@number"] = number, ["@payload"] = "Zażółć 😀 " + new string('x', 100)
                    });
            Source = new SQLiteTableCopyAdapter(SourcePath);
            Destination = new SQLiteTableCopyAdapter(DestinationPath);
        }

        internal Task<DbaTableCopyResult> CopyAsync(DbaTableCopyOptions options, CancellationToken cancellationToken = default)
            => new DbaTableCopyEngine().CopyAsync(Source, Destination, new[] { Definition }, options, cancellationToken);

        internal long DestinationCount()
        {
            using var sqlite = new SQLite();
            return Convert.ToInt64(sqlite.ExecuteScalar(DestinationPath, "SELECT COUNT(*) FROM DestinationRows"));
        }

        public void Dispose() => Directory.Delete(_directory, recursive: true);
    }
}
