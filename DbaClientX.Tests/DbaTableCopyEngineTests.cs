using System.Data;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public class DbaTableCopyEngineTests
{
    [Fact]
    public async Task CopyAsync_CopiesPagedRowsClearsDestinationAndReportsProgress()
    {
        var sourceTable = CreateRows(7);
        var source = new MemoryTableCopySource(sourceTable);
        var destination = new MemoryTableCopyDestination();
        var progress = new List<DbaTableCopyProgress>();

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows", new[] { "Id" }, "Rows") },
            new DbaTableCopyOptions
            {
                PageSize = 3,
                BatchSize = 2,
                BulkCopyTimeout = 30,
                ClearDestination = true,
                Progress = progress.Add
            });

        Assert.True(destination.ClearCalled);
        Assert.Equal(new[] { 3, 3, 3 }, source.RequestedPageSizes);
        Assert.Equal(new long[] { 0, 3, 6 }, source.RequestedOffsets);
        Assert.Equal(7, destination.Rows.Rows.Count);
        Assert.Equal(7, result.CopiedRows);
        Assert.Equal(7, result.SourceRows);
        Assert.Equal(7, result.DestinationRows);
        Assert.True(result.Verified);
        Assert.Collection(
            progress,
            first => AssertProgress(first, 3, 7, 3),
            second => AssertProgress(second, 6, 7, 3),
            third => AssertProgress(third, 7, 7, 1));
    }

    [Fact]
    public async Task CopyAsync_ReportsVerificationMismatch()
    {
        var source = new MemoryTableCopySource(CreateRows(2));
        var destination = new MemoryTableCopyDestination(destinationRowCountOverride: 1);

        var result = await new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") });

        var tableResult = Assert.Single(result.Tables);
        Assert.False(result.Verified);
        Assert.False(tableResult.Verified);
        Assert.Equal(2, tableResult.SourceRows);
        Assert.Equal(2, tableResult.CopiedRows);
        Assert.Equal(1, tableResult.DestinationRows);
    }

    [Theory]
    [InlineData(0, null, null)]
    [InlineData(100, 0, null)]
    [InlineData(100, null, 0)]
    public async Task CopyAsync_RejectsInvalidOptions(int pageSize, int? batchSize, int? timeout)
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("SourceRows", "DestinationRows") },
            new DbaTableCopyOptions
            {
                PageSize = pageSize,
                BatchSize = batchSize,
                BulkCopyTimeout = timeout
            }));
    }

    [Fact]
    public async Task CopyAsync_RejectsInvalidDefinitions()
    {
        var source = new MemoryTableCopySource(CreateRows(1));
        var destination = new MemoryTableCopyDestination();

        await Assert.ThrowsAsync<ArgumentException>(() => new DbaTableCopyEngine().CopyAsync(
            source,
            destination,
            new[] { new DbaTableCopyDefinition("", "DestinationRows") }));
    }

    private static void AssertProgress(DbaTableCopyProgress progress, long rowsCopied, long sourceRows, int pageRows)
    {
        Assert.Equal("Rows", progress.TableName);
        Assert.Equal(rowsCopied, progress.RowsCopied);
        Assert.Equal(sourceRows, progress.SourceRows);
        Assert.Equal(pageRows, progress.PageRows);
        Assert.Equal(rowsCopied * 100d / sourceRows, progress.PercentComplete);
    }

    private static DataTable CreateRows(int rows)
    {
        var table = new DataTable("SourceRows");
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("DisplayName", typeof(string));

        for (var i = 1; i <= rows; i++)
        {
            table.Rows.Add(i, $"Row {i}");
        }

        return table;
    }

    private sealed class MemoryTableCopySource : IDbaTableCopySource
    {
        private readonly DataTable _rows;

        public MemoryTableCopySource(DataTable rows)
            => _rows = rows;

        public List<long> RequestedOffsets { get; } = new();

        public List<int> RequestedPageSizes { get; } = new();

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(_rows.Rows.Count);

        public Task<DataTable> ReadPageAsync(DbaTableCopyPageRequest request, CancellationToken cancellationToken = default)
        {
            RequestedOffsets.Add(request.Offset);
            RequestedPageSizes.Add(request.PageSize);

            var page = _rows.Clone();
            page.TableName = request.Definition.DestinationName;
            foreach (var row in _rows.AsEnumerable().Skip((int)request.Offset).Take(request.PageSize))
            {
                page.ImportRow(row);
            }

            return Task.FromResult(page);
        }
    }

    private sealed class MemoryTableCopyDestination : IDbaTableCopyDestination
    {
        private readonly long? _destinationRowCountOverride;

        public MemoryTableCopyDestination(long? destinationRowCountOverride = null)
            => _destinationRowCountOverride = destinationRowCountOverride;

        public bool ClearCalled { get; private set; }

        public DataTable Rows { get; } = CreateRows(0);

        public Task ClearAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
        {
            ClearCalled = true;
            Rows.Rows.Clear();
            return Task.CompletedTask;
        }

        public Task WritePageAsync(DbaTableCopyDefinition definition, DataTable page, DbaTableCopyOptions options, CancellationToken cancellationToken = default)
        {
            foreach (DataRow row in page.Rows)
            {
                Rows.ImportRow(row);
            }

            return Task.CompletedTask;
        }

        public Task<long?> CountRowsAsync(DbaTableCopyDefinition definition, CancellationToken cancellationToken = default)
            => Task.FromResult<long?>(_destinationRowCountOverride ?? Rows.Rows.Count);
    }
}
