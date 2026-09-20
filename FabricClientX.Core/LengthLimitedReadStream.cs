namespace FabricClientX;

internal sealed class LengthLimitedReadStream : Stream
{
    private readonly Stream _inner;
    private readonly long _maximumBytes;
    private long _bytesRead;

    internal LengthLimitedReadStream(Stream inner, long maximumBytes)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _maximumBytes = maximumBytes > 0
            ? maximumBytes
            : throw new ArgumentOutOfRangeException(nameof(maximumBytes));
    }

    public override bool CanRead => _inner.CanRead;
    public override bool CanSeek => _inner.CanSeek;
    public override bool CanWrite => false;
    public override long Length => _inner.Length;
    public override long Position
    {
        get => _inner.Position;
        set => throw new NotSupportedException();
    }

    public override void Flush() => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        var read = _inner.Read(buffer, offset, LimitReadCount(count));
        RecordRead(read);
        return read;
    }

    public override async Task<int> ReadAsync(
        byte[] buffer,
        int offset,
        int count,
        CancellationToken cancellationToken)
    {
        var read = await _inner.ReadAsync(
            buffer,
            offset,
            LimitReadCount(count),
            cancellationToken).ConfigureAwait(false);
        RecordRead(read);
        return read;
    }

#if NET8_0_OR_GREATER
    public override int Read(Span<byte> buffer)
    {
        var read = _inner.Read(buffer[..LimitReadCount(buffer.Length)]);
        RecordRead(read);
        return read;
    }

    public override async ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
    {
        var read = await _inner.ReadAsync(
            buffer[..LimitReadCount(buffer.Length)],
            cancellationToken).ConfigureAwait(false);
        RecordRead(read);
        return read;
    }
#endif

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _inner.Dispose();
        }

        base.Dispose(disposing);
    }

    private int LimitReadCount(int requested)
    {
        var remainingIncludingProbe = _maximumBytes - _bytesRead + 1;
        return (int)Math.Min(requested, Math.Max(1, remainingIncludingProbe));
    }

    private void RecordRead(int count)
    {
        _bytesRead += count;
        if (_bytesRead > _maximumBytes)
        {
            throw new InvalidOperationException(
                "The service response exceeded the configured content-size limit.");
        }
    }
}
