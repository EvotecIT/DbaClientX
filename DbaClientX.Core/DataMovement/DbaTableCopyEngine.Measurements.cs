using System.Data;
using System.Diagnostics;
using DBAClientX.Diagnostics;

namespace DBAClientX.DataMovement;

public sealed partial class DbaTableCopyEngine
{
    // One explicitly passed owner per CopyAsync invocation. No ambient state or adapter wrappers.
    private sealed class CopyMeasurements
    {
        private readonly DbaTableCopyOptions _options;
        private readonly List<PhaseMeasurement> _phases = new();
        private PhaseMeasurement? _current;

        internal CopyMeasurements(DbaTableCopyOptions options) => _options = options;

        internal PhaseMeasurement BeginPhase(DbaTableCopyPhase phase, string? tableName = null)
        {
            var measurement = new PhaseMeasurement(this, _current, phase, tableName);
            if (_options.CollectPerformanceStatistics) _phases.Add(measurement);
            _current = measurement;
            return measurement;
        }

        internal void ReadPage(DbaTableCopyPageRequest request, DataTable page, bool destination)
        {
            if (!_options.CollectPerformanceStatistics || _current == null) return;
            long bytes = DbaTableCopyPageReader.EstimatePayloadBytes(page);
            if (destination)
            {
                _current.DestinationPageCount++;
                _current.DestinationRowsRead += page.Rows.Count;
                _current.EstimatedDestinationPayloadBytes += bytes;
            }
            else
            {
                _current.SourcePageCount++;
                if (request.ContinuationToken == null) _current.SourcePageStreamCount++;
                _current.SourceRowsRead += page.Rows.Count;
                _current.EstimatedSourcePayloadBytes += bytes;
            }
        }

        internal void WritePage(DataTable page)
        {
            if (!_options.CollectPerformanceStatistics || _current == null) return;
            _current.WrittenPageCount++;
            _current.RowsWritten += page.Rows.Count;
            _current.EstimatedWrittenPayloadBytes += DbaTableCopyPageReader.EstimatePayloadBytes(page);
        }

        internal void ReportProgress(string tableName, long rows, long? totalRows, int pageRows, long processedRows)
        {
            if (_options.Progress == null) return;
            _options.Progress(new DbaTableCopyProgress(tableName, rows, totalRows, pageRows)
            {
                Phase = _current!.Phase,
                Elapsed = _current.Elapsed,
                RowsProcessedThisPass = processedRows
            });
        }

        internal DbaTableCopyPerformance? Complete()
            => !_options.CollectPerformanceStatistics ? null : new DbaTableCopyPerformance
            {
                Phases = Array.AsReadOnly(_phases.Select(static phase => phase.Snapshot()).ToArray())
            };

        internal sealed class PhaseMeasurement : IDisposable
        {
            private readonly CopyMeasurements _owner;
            private readonly PhaseMeasurement? _parent;
            private readonly string? _tableName;
            private readonly long _started = Stopwatch.GetTimestamp();
            private long _nestedTicks;
            private long? _stopped;

            internal PhaseMeasurement(CopyMeasurements owner, PhaseMeasurement? parent, DbaTableCopyPhase phase, string? tableName)
            {
                _owner = owner;
                _parent = parent;
                Phase = phase;
                _tableName = tableName;
            }

            internal DbaTableCopyPhase Phase { get; }
            internal TimeSpan Elapsed => FromTicks((_stopped ?? Stopwatch.GetTimestamp()) - _started);
            internal long SourcePageCount, SourcePageStreamCount, SourceRowsRead, EstimatedSourcePayloadBytes;
            internal long DestinationPageCount, DestinationRowsRead, EstimatedDestinationPayloadBytes;
            internal long WrittenPageCount, RowsWritten, EstimatedWrittenPayloadBytes;

            public void Dispose()
            {
                if (_stopped.HasValue) return;
                _stopped = Stopwatch.GetTimestamp();
                if (_parent != null) _parent._nestedTicks += _stopped.Value - _started;
                _owner._current = _parent;
            }

            internal DbaTableCopyPhaseStatistics Snapshot() => new()
            {
                Phase = Phase,
                TableName = _tableName == null ? null : DbaClientXDiagnostics.SanitizeLogicalName(_tableName),
                Duration = FromTicks(_stopped!.Value - _started - _nestedTicks),
                SourcePageCount = SourcePageCount,
                SourcePageStreamCount = SourcePageStreamCount,
                SourceRowsRead = SourceRowsRead,
                EstimatedSourcePayloadBytes = EstimatedSourcePayloadBytes,
                DestinationPageCount = DestinationPageCount,
                DestinationRowsRead = DestinationRowsRead,
                EstimatedDestinationPayloadBytes = EstimatedDestinationPayloadBytes,
                WrittenPageCount = WrittenPageCount,
                RowsWritten = RowsWritten,
                EstimatedWrittenPayloadBytes = EstimatedWrittenPayloadBytes
            };

            private static TimeSpan FromTicks(long ticks) => TimeSpan.FromSeconds((double)ticks / Stopwatch.Frequency);
        }
    }
}
