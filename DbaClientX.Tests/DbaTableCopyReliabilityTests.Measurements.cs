using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopyReliabilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_VerifiedRunAccountsSourceProofAndCommittedWritesSeparately(bool measure)
    {
        using var fixture = new Fixture();
        var result = await fixture.CopyAsync(new()
        {
            VerifyContent = true, CheckpointId = "measure", PageSize = 3, CollectPerformanceStatistics = measure
        });
        Assert.True(result.Verified);
        Assert.Equal(12, fixture.DestinationCount());
        if (!measure)
        {
            Assert.Null(result.Performance);
            Assert.Null(result.Manifest!.Performance);
            return;
        }
        var statistics = Assert.IsType<DbaTableCopyPerformance>(result.Performance);
        Assert.Equal(24, statistics.SourceRowsRead);
        Assert.Equal(12, statistics.DestinationRowsRead);
        Assert.Equal(12, statistics.RowsWritten);
        Assert.Equal(statistics.EstimatedWrittenPayloadBytes * 2, statistics.EstimatedSourcePayloadBytes);
        Assert.Equal(12, Assert.Single(statistics.Phases, phase => phase.Phase == DbaTableCopyPhase.ValidateSource).SourceRowsRead);
        var copy = Assert.Single(statistics.Phases, phase => phase.Phase == DbaTableCopyPhase.Copy);
        Assert.Equal(4, copy.WrittenPageCount);
        Assert.Equal(12, copy.SourceRowsRead);
        Assert.Equal(12, Assert.Single(statistics.Phases, phase => phase.Phase == DbaTableCopyPhase.VerifyDestination).DestinationRowsRead);
        Assert.Same(statistics, result.Manifest!.Performance);
    }

    [Fact]
    public async Task CopyAsync_ResumeCountsOnlyNewWritesAndCompletedResumeReusesDestinationProof()
    {
        using var fixture = new Fixture();
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.CopyAsync(new()
        {
            CheckpointId = "resume-measure", PageSize = 3,
            Progress = snapshot => { if (snapshot.Phase == DbaTableCopyPhase.Copy) cancellation.Cancel(); }
        }, cancellation.Token));
        var progress = new List<DbaTableCopyProgress>();
        var resumed = await fixture.CopyAsync(new()
        {
            CheckpointId = "resume-measure", Resume = true, PageSize = 3,
            CollectPerformanceStatistics = true, Progress = progress.Add
        });
        Assert.True(resumed.Verified);
        Assert.Equal(9, resumed.Performance!.RowsWritten);
        Assert.Equal(21, resumed.Performance.SourceRowsRead);
        var first = progress.First(snapshot => snapshot.Phase == DbaTableCopyPhase.Copy);
        Assert.Equal(6, first.RowsCopied);
        Assert.Equal(3, first.RowsProcessedThisPass);
        Assert.Equal(3 / first.Elapsed!.Value.TotalSeconds, first.RowsPerSecond);
        Assert.Equal(12, fixture.DestinationCount());

        var complete = await fixture.CopyAsync(new()
        {
            CheckpointId = "resume-measure", Resume = true, PageSize = 3, CollectPerformanceStatistics = true
        });
        Assert.True(complete.Verified);
        Assert.Equal(0, complete.Performance!.RowsWritten);
        Assert.Equal(12, complete.Performance.SourceRowsRead);
        Assert.Equal(12, complete.Performance.DestinationRowsRead);
        Assert.Single(complete.Performance.Phases, phase => phase.Phase == DbaTableCopyPhase.VerifyDestination);
    }

    [Fact]
    public async Task CopyAsync_MultiTableOverwriteMeasuresSeparateDestructivePreflightPass()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath,
            "CREATE TABLE OtherRows (GroupName TEXT NOT NULL, Number INTEGER NOT NULL, Payload TEXT NULL, PRIMARY KEY(GroupName,Number))");
        var result = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination,
            new[] { fixture.Definition, fixture.Definition with { DestinationName = "OtherRows" } },
            new() { ClearDestination = true, VerifyContent = true, PageSize = 3, CollectPerformanceStatistics = true });

        Assert.True(result.Verified);
        Assert.Equal(24, result.Performance!.RowsWritten);
        Assert.Equal(72, result.Performance.SourceRowsRead);
        var preflight = result.Performance.Phases.Where(phase => phase.Phase == DbaTableCopyPhase.PreflightSource);
        Assert.Equal(24, preflight.Sum(phase => phase.SourceRowsRead));
        Assert.Equal(12, fixture.DestinationCount());
        Assert.Equal(12L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT COUNT(*) FROM OtherRows")));
    }
}
