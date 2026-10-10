namespace DBAClientX.Dbf.Internal;

internal sealed class DbfMemoReader : IDisposable
{
    private readonly Stream _stream;
    private readonly long _start, _length;
    private readonly bool _fpt;
    private readonly int _blockSize;
    private readonly DbfReadOptions _options;
    private readonly CancellationToken _token;
    private long _materializedBytes;

    internal DbfMemoReader(Stream stream, bool fpt, DbfReadOptions options, CancellationToken token)
    {
        if (!stream.CanSeek || !stream.CanRead) throw new ArgumentException("Memo storage must be seekable and readable.", nameof(stream));
        _stream = stream; _start = stream.Position; _length = stream.Length - _start;
        _options = options; _token = token; _fpt = fpt;
        if (_length < 512 || _length > options.MaxMemoFileBytes)
            throw new InvalidDataException("Memo storage is truncated or exceeds MaxMemoFileBytes.");
        var header = new byte[8];
        DbfIO.ReadExactly(stream, header, 0, header.Length, token);
        _blockSize = fpt ? header[6] << 8 | header[7] : 512;
        if (_blockSize < 1) throw new InvalidDataException("FPT block size is invalid.");
    }

    internal byte[] Read(uint pointer, bool text)
    {
        _token.ThrowIfCancellationRequested();
        long offset = pointer * (long)_blockSize;
        if (offset < 512 || offset >= _length) throw new InvalidDataException("DBF memo pointer is outside payload storage.");
        _stream.Position = checked(_start + offset);
        return _fpt ? ReadFpt(offset, text) : ReadDbt();
    }

    private byte[] ReadFpt(long offset, bool text)
    {
        if (_length - offset < 8) throw new InvalidDataException("FPT memo block header is truncated.");
        var header = new byte[8];
        DbfIO.ReadExactly(_stream, header, 0, header.Length, _token);
        uint kind = DbfIO.BigUInt32(header, 0);
        uint length = DbfIO.BigUInt32(header, 4);
        if (kind is not (0 or 1) || (text && kind != 1)) throw new NotSupportedException("FPT memo block type does not match the requested field.");
        if (length > _options.MaxMemoBytes || length > _length - offset - 8) throw new InvalidDataException("FPT payload is truncated or exceeds MaxMemoBytes.");
        Debit(length);
        var bytes = new byte[(int)length];
        DbfIO.ReadExactly(_stream, bytes, 0, bytes.Length, _token);
        return bytes;
    }

    private byte[] ReadDbt()
    {
        using var output = new MemoryStream();
        var chunk = new byte[512];
        bool marker = false;
        while (_stream.Position - _start < _length)
        {
            _token.ThrowIfCancellationRequested();
            long remaining = _length - (_stream.Position - _start);
            int count = (int)Math.Min(chunk.Length, Math.Min(remaining, (long)_options.MaxMemoBytes - output.Length + 2));
            if (count <= 0) throw new InvalidDataException("DBT memo exceeds MaxMemoBytes.");
            DbfIO.ReadExactly(_stream, chunk, 0, count, _token);
            for (int index = 0; index < count; index++)
            {
                byte value = chunk[index];
                if (marker && value == 0x1a) { Debit(output.Length); return output.ToArray(); }
                if (marker) WriteByte(0x1a);
                marker = value == 0x1a;
                if (!marker) WriteByte(value);
            }
        }
        throw new InvalidDataException("DBT memo terminator is missing.");

        void WriteByte(byte value)
        {
            if (output.Length >= _options.MaxMemoBytes || output.Length >= _options.MaxTotalMemoBytes - _materializedBytes)
                throw new InvalidDataException("DBT memo exceeds its per-value or aggregate byte limit.");
            output.WriteByte(value);
        }
    }

    private void Debit(long count)
    {
        if (count > _options.MaxTotalMemoBytes - _materializedBytes) throw new InvalidDataException("DBF memo payloads exceed MaxTotalMemoBytes.");
        _materializedBytes += count;
    }

    public void Dispose() => _stream.Dispose();
}
