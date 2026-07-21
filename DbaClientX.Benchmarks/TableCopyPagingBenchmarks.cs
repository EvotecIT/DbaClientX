using System.Data;
using BenchmarkDotNet.Attributes;
using DBAClientX.DataMovement;

namespace DbaClientX.Benchmarks;

/// <summary>Compares the cursor copy engine with the previous offset-loop behavior.</summary>
[MemoryDiagnoser]
public class TableCopyPagingBenchmarks
{
    private DataTable _rows = null!;
    private DbaTableCopyDefinition _definition = null!;

    [Params(10_000)]
    public int RowCount { get; set; }

    [Params(100, 1000)]
    public int PageSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _rows = new DataTable("SourceRows");
        _rows.Columns.Add("Id", typeof(int));
        _rows.Columns.Add("DisplayName", typeof(string));
        for (var index = 0; index < RowCount; index++)
        {
            _rows.Rows.Add(index, $"Row {index}");
        }

        _definition = new DbaTableCopyDefinition("SourceRows", "DestinationRows");
    }

    [GlobalCleanup]
    public void Cleanup()
        => _rows.Dispose();

    [Benchmark(Baseline = true)]
    public async Task<long> LegacyOffsetLoop()
    {
        var destination = new CountingDestination();
        long copied = 0;
        long offset = 0;
        while (copied < _rows.Rows.Count)
        {
            using var page = ReadOffsetPage(offset, PageSize);
            if (page.Rows.Count == 0)
            {
                break;
            }

            await destination.WritePageAsync(
                    _definition,
                    page,
                    new DbaTableCopyOptions { PageSize = PageSize },
                    CancellationToken.None)
                .ConfigureAwait(false);
            copied += page.Rows.Count;
            offset += page.Rows.Count;
            if (page.Rows.Count < PageSize)
            {
                break;
            }
        }

        return copied;
    }

    [Benchmark]
    public async Task<long> CursorCopyEngine()
    {
        var result = await new DbaTableCopyEngine().CopyAsync(
                new CursorSource(_rows),
                new CountingDestination(),
                new[] { _definition },
                new DbaTableCopyOptions { PageSize = PageSize })
            .ConfigureAwait(false);
        return result.CopiedRows;
    }

    private DataTable ReadOffsetPage(long offset, int pageSize)
    {
        var page = _rows.Clone();
        foreach (var row in _rows.AsEnumerable().Skip((int)offset).Take(pageSize))
        {
            page.ImportRow(row);
        }

        return page;
    }

    private sealed class CursorSource : IDbaTableCopySource
    {
        private readonly DataTable _rows;

        public CursorSource(DataTable rows)
            => _rows = rows;

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(_rows.Rows.Count);

        public Task<DbaTableCopyPage> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
        {
#pragma warning disable CS0618
            var offset = request.Offset;
#pragma warning restore CS0618
            var page = _rows.Clone();
            foreach (var row in _rows.AsEnumerable().Skip((int)offset).Take(request.PageSize))
            {
                page.ImportRow(row);
            }

            var nextOffset = offset + page.Rows.Count;
            return Task.FromResult(DbaTableCopyPage.FromOffset(
                page,
                nextOffset < _rows.Rows.Count ? nextOffset : null));
        }
    }

    private sealed class CountingDestination : IDbaTableCopyDestination
    {
        private long _rows;

        public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        {
            _rows = 0;
            return Task.CompletedTask;
        }

        public Task WritePageAsync(
            DbaTableCopyDefinition definition,
            DataTable page,
            DbaTableCopyOptions options,
            CancellationToken cancellationToken = default)
        {
            _rows += page.Rows.Count;
            return Task.CompletedTask;
        }

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(_rows);
    }
}
