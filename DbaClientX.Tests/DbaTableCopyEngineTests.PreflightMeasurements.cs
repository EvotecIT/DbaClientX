using System.Data;
using System.Diagnostics;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public partial class DbaTableCopyEngineTests
{
    [Theory]
    [InlineData(true, true)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task CopyAsync_VerifiedCopyMeasuresSchemaPreflightOutsideSourceValidation(bool session, bool clear)
    {
        using var rows = CreateRows(4);
        PreflightMeasurementDestination destination = session
            ? new SessionMeasurementDestination()
            : new SchemaMeasurementDestination();
        var result = await new DbaTableCopyEngine().CopyAsync(new MemoryTableCopySource(rows), destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }, "Rows") { UseKeysetPagination = true } },
            new() { VerifyContent = true, ClearDestination = clear, PageSize = 2, CollectPerformanceStatistics = true });

        Assert.True(result.Verified);
        var performance = Assert.IsType<DbaTableCopyPerformance>(result.Performance);
        var validation = Assert.Single(performance.Phases, phase => phase.Phase == DbaTableCopyPhase.ValidateSource);
        Assert.Equal(4, validation.SourceRowsRead);
        var preflight = performance.Phases.Where(phase => phase.Phase == DbaTableCopyPhase.PreflightSource).ToArray();
        Assert.NotEmpty(preflight);
        Assert.All(preflight, phase =>
        {
            Assert.Equal("Rows", phase.TableName);
            Assert.Equal(0, phase.SourceRowsRead);
            Assert.Equal(0, phase.RowsWritten);
        });
        // The measured boundary includes provider waits in opening, validating, and rolling back.
        // Compare observed waits, without an upper bound that depends on scheduler load.
        double preflightSeconds = preflight.Sum(phase => phase.Duration.TotalSeconds);
        Assert.True(preflightSeconds >= destination.WaitDuration.TotalSeconds - TimeSpan.FromTicks(10).TotalSeconds);
        Assert.True(performance.Phases.Sum(phase => phase.Duration.TotalSeconds) <= result.Duration.TotalSeconds);
    }

    private abstract class PreflightMeasurementDestination : IDbaTableCopyDestination, IDbaTableCopySource, IDbaTableCopyPagePreflightDestination
    {
        private readonly MemoryTableCopyDestination _destination = new();
        private long _waitTicks;
        private bool _sourceProofComplete;
        internal TimeSpan WaitDuration => TimeSpan.FromSeconds((double)_waitTicks / Stopwatch.Frequency);

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        {
            _sourceProofComplete = true;
            return _destination.CountRowsAsync(definition, cancellationToken);
        }

        public void ValidatePage(DbaTableCopyDefinition definition, DataTable page)
        {
            if (_sourceProofComplete) return;
            long started = Stopwatch.GetTimestamp();
            Thread.Sleep(10);
            _waitTicks += Stopwatch.GetTimestamp() - started;
        }

        public Task<DbaTableCopyPage> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
            => new MemoryTableCopySource(_destination.Rows).ReadPageAsync(request, cancellationToken);

        public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
            => _destination.ClearAsync(definition, cancellationToken);

        public Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options,
            CancellationToken cancellationToken = default)
            => _destination.WritePageAsync(definition, page, options, cancellationToken);

        protected async Task WaitAsync(CancellationToken cancellationToken = default)
        {
            long started = Stopwatch.GetTimestamp();
            await Task.Delay(20, cancellationToken);
            _waitTicks += Stopwatch.GetTimestamp() - started;
        }
    }

    private sealed class SchemaMeasurementDestination : PreflightMeasurementDestination, IDbaTableCopySchemaPreflightDestination
    {
        public Task ValidateSchemaAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options,
            CancellationToken cancellationToken) => WaitAsync(cancellationToken);
    }

    private sealed class SessionMeasurementDestination : PreflightMeasurementDestination, IDbaTableCopySchemaPreflightSessionDestination
    {
        public async Task<IDbaTableCopySchemaPreflightSession> OpenSchemaPreflightSessionAsync(
            DbaTableCopyDefinition definition, DataTable firstPage, DbaTableCopyOptions options, CancellationToken cancellationToken)
        {
            await WaitAsync(cancellationToken);
            return new Session(this);
        }

        private sealed class Session(SessionMeasurementDestination owner) : IDbaTableCopySchemaPreflightSession
        {
            public Task ValidatePageAsync(DataTable page, CancellationToken cancellationToken) => owner.WaitAsync(cancellationToken);
            public async ValueTask DisposeAsync() => await owner.WaitAsync();
        }
    }
}
