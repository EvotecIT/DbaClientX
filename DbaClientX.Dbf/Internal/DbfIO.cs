namespace DBAClientX.Dbf.Internal;

internal static class DbfIO
{
    internal static int UInt16(byte[] bytes, int offset) => bytes[offset] | bytes[offset + 1] << 8;
    internal static uint UInt32(byte[] bytes, int offset) => (uint)(bytes[offset] | bytes[offset + 1] << 8 | bytes[offset + 2] << 16 | bytes[offset + 3] << 24);
    internal static uint BigUInt32(byte[] bytes, int offset) => (uint)(bytes[offset] << 24 | bytes[offset + 1] << 16 | bytes[offset + 2] << 8 | bytes[offset + 3]);

    internal static void ReadExactly(Stream stream, byte[] bytes, int offset, int count, CancellationToken cancellationToken)
    {
        while (count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int read = stream.Read(bytes, offset, Math.Min(count, 4096));
            if (read == 0) throw new InvalidDataException("DBF storage is truncated.");
            offset += read; count -= read;
        }
        cancellationToken.ThrowIfCancellationRequested();
    }

    internal static async Task ReadExactlyAsync(Stream stream, byte[] bytes, int offset, int count, CancellationToken cancellationToken)
    {
        while (count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int read = await stream.ReadAsync(bytes, offset, Math.Min(count, 4096), cancellationToken).ConfigureAwait(false);
            if (read == 0) throw new InvalidDataException("DBF storage is truncated.");
            offset += read; count -= read;
        }
        cancellationToken.ThrowIfCancellationRequested();
    }

    internal static int GetHeaderLength(byte[] fixedHeader, DbfReadOptions options)
    {
        int length = UInt16(fixedHeader, 8);
        if (length < 33 || length > options.MaxInputBytes) throw new InvalidDataException("DBF header length is invalid or exceeds MaxInputBytes.");
        return length;
    }
}
