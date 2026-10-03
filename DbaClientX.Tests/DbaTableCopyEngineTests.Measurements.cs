using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public partial class DbaTableCopyEngineTests
{
    [Fact]
    public async Task CopyAsync_MeasuresReusedPreflightPageOnceAndKeepsManifestRedacted()
    {
        using var rows = CreateRows(7);
        var progress = new List<DbaTableCopyProgress>();
        var result = await new DbaTableCopyEngine().CopyAsync(new MemoryTableCopySource(rows),
            new MemoryTableCopyDestination(),
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", LogicalName: "Rows\nPRIVATE") },
            new() { ClearDestination = true, PageSize = 3, CollectPerformanceStatistics = true, Progress = progress.Add });

        var statistics = Assert.IsType<DbaTableCopyPerformance>(result.Performance);
        Assert.Same(statistics, result.Manifest!.Performance);
        Assert.Equal(3, statistics.SourcePageCount);
        Assert.Equal(7, statistics.SourceRowsRead);
        Assert.Equal(7, statistics.RowsWritten);
        Assert.Equal(7 * 178, statistics.EstimatedSourcePayloadBytes);
        Assert.Equal(statistics.EstimatedSourcePayloadBytes, statistics.EstimatedWrittenPayloadBytes);
        var preflight = Assert.Single(statistics.Phases, phase => phase.Phase == DbaTableCopyPhase.PreflightSource);
        var copy = Assert.Single(statistics.Phases, phase => phase.Phase == DbaTableCopyPhase.Copy);
        Assert.Equal(3, preflight.SourceRowsRead);
        Assert.Equal(4, copy.SourceRowsRead);
        Assert.Equal(1, preflight.SourcePageStreamCount);
        Assert.Equal(0, copy.SourcePageStreamCount);
        Assert.All(statistics.Phases, phase =>
        {
            Assert.True(phase.Duration >= TimeSpan.Zero);
            Assert.DoesNotContain("\n", phase.TableName ?? string.Empty);
        });
        Assert.True(statistics.Phases.Sum(phase => phase.Duration.TotalSeconds) <= result.Duration.TotalSeconds);
        Assert.Equal(new long?[] { 3, 6, 7 }, progress.Select(snapshot => snapshot.RowsProcessedThisPass));
        Assert.All(progress, snapshot => Assert.True(snapshot.RowsPerSecond > 0));
        Assert.Equal(TimeSpan.Zero, progress[^1].EstimatedRemaining);
    }

    [Fact]
    public async Task CopyAsync_UnknownCountReportsRateWithoutInventingETAOrEnablingStatistics()
    {
        using var rows = CreateRows(4);
        var progress = new List<DbaTableCopyProgress>();
        var result = await new DbaTableCopyEngine().CopyAsync(
            new MemoryTableCopySource(rows) { ReturnUnknownSourceRowCount = true },
            new MemoryTableCopyDestination(), new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new() { PageSize = 2, Progress = progress.Add });

        Assert.Null(result.Performance);
        Assert.Null(result.Manifest!.Performance);
        Assert.All(progress, snapshot =>
        {
            Assert.Null(snapshot.SourceRows);
            Assert.Null(snapshot.PercentComplete);
            Assert.Null(snapshot.EstimatedRemaining);
            Assert.True(snapshot.RowsPerSecond > 0);
            Assert.True(snapshot.Elapsed > TimeSpan.Zero);
        });
    }

    [Fact]
    public void Progress_RateUsesCurrentPassAndETAExcludesAlreadyCommittedRows()
    {
        var snapshot = new DbaTableCopyProgress("Rows", 90, 100, 5)
        {
            RowsProcessedThisPass = 5, Elapsed = TimeSpan.FromSeconds(2)
        };
        Assert.Equal(2.5, snapshot.RowsPerSecond);
        Assert.Equal(TimeSpan.FromSeconds(4), snapshot.EstimatedRemaining);
        Assert.Null(new DbaTableCopyProgress("Rows", 1, 100, 1).RowsPerSecond);
        Assert.Null(new DbaTableCopyProgress("Rows", 1, 100, 1).EstimatedRemaining);
        Assert.Null((snapshot with { SourceRows = long.MaxValue, Elapsed = TimeSpan.MaxValue }).EstimatedRemaining);
        var (table, copied, total, page) = snapshot;
        Assert.Equal(("Rows", 90L, (long?)100L, 5), (table, copied, total, page));
    }
}
