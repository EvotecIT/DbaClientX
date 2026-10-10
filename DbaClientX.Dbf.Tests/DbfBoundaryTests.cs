using System.Text;
using DBAClientX.Dbf;

namespace DbaClientX.Dbf.Tests;

public sealed class DbfBoundaryTests
{
    private static byte[] Table(string name = "db3") => File.ReadAllBytes(DbfFixtureTests.Fixture(name + ".dbf"));
    private static MemoryStream Memo(string name = "db3") => new(File.ReadAllBytes(
        DbfFixtureTests.Fixture(name + (name == "db3" ? ".dbt" : ".fpt"))));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void StreamsStartAtCurrentPositionAndFollowOwnershipPolicy(bool leaveOpen)
    {
        using var table = new MemoryStream();
        table.Write(new byte[7]); table.Write(Table()); table.Position = 7;
        using var sourceMemo = Memo();
        using var memo = new MemoryStream();
        memo.Write(new byte[11]); sourceMemo.CopyTo(memo); memo.Position = 11;
        var reader = DbfDataReader.Open(table, new DbfReadOptions { LeaveOpen = leaveOpen }, memo);
        Assert.True(reader.Read());
        Assert.Equal("First memo\r\nSecond line: naïve", reader.GetString(4));
        reader.Close(); reader.Dispose();
        Assert.Equal(leaveOpen, table.CanRead);
        Assert.Equal(leaveOpen, memo.CanRead);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailedOpenReleasesOnlyOwnedStreams(bool leaveOpen)
    {
        using var table = new MemoryStream(new byte[33]);
        using var memo = Memo();
        await Assert.ThrowsAsync<InvalidDataException>(() => DbfDataReader.OpenAsync(table,
            new DbfReadOptions { LeaveOpen = leaveOpen }, memo));
        Assert.Equal(leaveOpen, table.CanRead);
        Assert.Equal(leaveOpen, memo.CanRead);
    }

    [Fact]
    public async Task NonseekableTableUsesTrueAsyncReadsAndDetectsTruncation()
    {
        using var stream = new AsyncOnlyStream(Table());
        using var memo = Memo();
        await using var reader = await DbfDataReader.OpenAsync(stream, memoStream: memo);
        Assert.True(await reader.ReadAsync());
        Assert.Equal("Café £", reader.GetString(0));
        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
        byte[] truncated = Table()[..^3];
        using var broken = new AsyncOnlyStream(truncated);
        await using var other = await DbfDataReader.OpenAsync(broken);
        Assert.True(await other.ReadAsync());
        await Assert.ThrowsAsync<InvalidDataException>(() => other.ReadAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => other.ReadAsync());
    }

    [Fact]
    public async Task PreCancelledReadPreservesCurrentRowAndOpenTokenRemainsEffective()
    {
        using var source = new MemoryStream(Table());
        using var cancellation = new CancellationTokenSource();
        using var reader = DbfDataReader.Open(source, cancellationToken: cancellation.Token);
        Assert.True(reader.Read());
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.ReadAsync(new CancellationToken(true)));
        Assert.Equal("Café £", reader.GetString(0));
        cancellation.Cancel();
        Assert.ThrowsAny<OperationCanceledException>(() => reader.Read());
        Assert.ThrowsAny<OperationCanceledException>(() => reader.GetValue(0));
    }

    [Fact]
    public void MissingMemoIsReportedOnlyWhenAReferencedValueIsRequested()
    {
        using var source = new MemoryStream(Table());
        using var reader = DbfDataReader.Open(source);
        Assert.True(reader.Read());
        Assert.Equal("Café £", reader.GetString(0));
        Assert.Throws<InvalidDataException>(() => reader.GetString(4));
        Assert.Throws<InvalidOperationException>(() => reader.Read());
    }

    [Theory]
    [InlineData("db3")]
    [InlineData("fp")]
    public void MemoLimitsRejectOversizePayloadsAndCountMaterializationOncePerField(string name)
    {
        using var source = new MemoryStream(Table(name));
        using var memo = Memo(name);
        using var limited = DbfDataReader.Open(source, new DbfReadOptions { MaxMemoBytes = 4 }, memo);
        Assert.True(limited.Read());
        Assert.Throws<InvalidDataException>(() => limited.GetString(4));

        using var secondSource = new MemoryStream(Table(name));
        using var secondMemo = Memo(name);
        using var aggregate = DbfDataReader.Open(secondSource,
            new DbfReadOptions { IncludeDeletedRecords = true, MaxTotalMemoBytes = 30 }, secondMemo);
        Assert.True(aggregate.Read());
        Assert.Equal(30, aggregate.GetString(4).Length);
        Assert.Equal(aggregate.GetValue(4), aggregate.GetValue(4));
        Assert.True(aggregate.Read());
        Assert.Throws<InvalidDataException>(() => aggregate.GetString(4));
    }

    [Fact]
    public void EncodingIsExplicitStrictAndSnapshotted()
    {
        byte[] data = Table(); data[29] = 0;
        using var source = new MemoryStream(data);
        using var ascii = DbfDataReader.Open(source);
        Assert.True(ascii.Read());
        Assert.Throws<InvalidDataException>(() => ascii.GetValue(0));
        data[29] = 0xfe;
        using var unknown = new MemoryStream(data);
        Assert.Throws<NotSupportedException>(() => DbfDataReader.Open(unknown));
        using var explicitSource = new MemoryStream(data);
        var options = new DbfReadOptions { Encoding = Encoding.Latin1 };
        using var reader = DbfDataReader.Open(explicitSource, options);
        options.Encoding = Encoding.ASCII;
        Assert.True(reader.Schema.EncodingOverridden);
        Assert.True(reader.Read());
        Assert.Equal("Café £", reader.GetString(0));
    }

    [Theory]
    [InlineData(0, 0x8b, typeof(NotSupportedException))]
    [InlineData(43, 'Q', typeof(NotSupportedException))]
    [InlineData(8, 32, typeof(InvalidDataException))]
    [InlineData(4, 100, typeof(InvalidDataException))]
    public void UnsupportedDialectsAndMalformedStructuresFailExplicitly(int offset, byte value, Type error)
    {
        byte[] data = Table(); data[offset] = value;
        using var source = new MemoryStream(data);
        Assert.Throws(error, () => DbfDataReader.Open(source));
    }

    [Fact]
    public void DeclaredAndActualStorageRespectReaderBudgets()
    {
        using var source = new MemoryStream(Table());
        Assert.Throws<InvalidDataException>(() => DbfDataReader.Open(source, new DbfReadOptions { MaxRecords = 2 }));
        source.Position = 0;
        Assert.Throws<InvalidDataException>(() => DbfDataReader.Open(source, new DbfReadOptions { MaxInputBytes = source.Length - 1 }));
        source.Position = 0;
        Assert.Throws<InvalidDataException>(() => DbfDataReader.Open(source, new DbfReadOptions { MaxFields = 4 }));
    }

    [Fact]
    public void FptPointersAndLengthsCannotEscapeTheSidecar()
    {
        byte[] data = Table("vfp");
        int firstRecord = BitConverter.ToUInt16(data, 8);
        // The independently produced schema places NOTES after C(24), I(4), B(8), Y(8), T(8).
        int memoPointer = firstRecord + 1 + 24 + 4 + 8 + 8 + 8;
        Array.Copy(BitConverter.GetBytes(uint.MaxValue), 0, data, memoPointer, 4);
        using var source = new MemoryStream(data);
        using var memo = Memo("vfp");
        using var reader = DbfDataReader.Open(source, memoStream: memo);
        Assert.True(reader.Read());
        Assert.Throws<InvalidDataException>(() => reader.GetValue(5));

        byte[] correctTable = Table("vfp");
        byte[] memoBytes = File.ReadAllBytes(DbfFixtureTests.Fixture("vfp.fpt"));
        int blockSize = (memoBytes[6] << 8) | memoBytes[7];
        int block = checked((int)BitConverter.ToUInt32(correctTable, memoPointer) * blockSize);
        Array.Fill(memoBytes, (byte)0xff, block + 4, 4);
        using var secondSource = new MemoryStream(correctTable);
        using var brokenMemo = new MemoryStream(memoBytes);
        using var other = DbfDataReader.Open(secondSource, memoStream: brokenMemo);
        Assert.True(other.Read());
        Assert.Throws<InvalidDataException>(() => other.GetValue(5));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BacklinkMetadataUsesNativeFirstByteSentinel(bool associated)
    {
        byte[] data = Table("vfp");
        int terminator = 32;
        while (data[terminator] != 0x0d) terminator += 32;
        data[terminator + 1] = associated ? (byte)'d' : (byte)0;
        data[terminator + 2] = (byte)'b';
        using var source = new MemoryStream(data);
        using var reader = DbfDataReader.Open(source);
        Assert.Equal(associated, reader.Schema.HasDatabaseBacklink);
    }

    [Fact]
    public async Task CancellationAfterPartialRowIoFaultsTheReader()
    {
        using var source = new CancellingStream(Table());
        await using var reader = await DbfDataReader.OpenAsync(source);
        source.CancelNextRead = true;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => reader.ReadAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => reader.ReadAsync());
        Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
    }

    private sealed class CancellingStream(byte[] bytes) : MemoryStream(bytes)
    {
        internal bool CancelNextRead { get; set; }
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token)
        {
            if (!CancelNextRead) return base.ReadAsync(buffer, offset, count, token);
            base.Read(buffer, offset, 1);
            return Task.FromException<int>(new OperationCanceledException());
        }
    }

    private sealed class AsyncOnlyStream(byte[] bytes) : Stream
    {
        private readonly MemoryStream _inner = new(bytes);
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new InvalidOperationException("Synchronous I/O is forbidden.");
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken token) =>
            _inner.ReadAsync(buffer, offset, Math.Min(count, 7), token);
        public override void Flush() => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long length) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        protected override void Dispose(bool disposing) { if (disposing) _inner.Dispose(); base.Dispose(disposing); }
    }
}
