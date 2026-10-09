using DBAClientX.Dbf.Internal;

namespace DBAClientX.Dbf;

public sealed partial class DbfDataReader
{
    /// <summary>Opens a table and, when present, its same-stem lowercase or uppercase DBT/FPT sidecar. This reader owns both files.</summary>
    /// <remarks>A missing sidecar does not prevent schema or ordinary-column reading. Accessing a nonempty memo requires it.</remarks>
    public static DbfDataReader Open(string path, DbfReadOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("A DBF path is required.", nameof(path));
        cancellationToken.ThrowIfCancellationRequested();
        DbfReadOptions snapshot = (options ?? new DbfReadOptions()).Snapshot();
        var table = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
        DbfDataReader? reader = null;
        try
        {
            reader = OpenCore(table, null, snapshot, ownsStreams: true, cancellationToken);
            reader.OpenMemoFile(path);
            return reader;
        }
        catch { if (reader != null) reader.Dispose(); else table.Dispose(); throw; }
    }

    /// <summary>Opens a readable table stream at its current position, with an optional seekable memo stream at its current position.</summary>
    /// <remarks><see cref="DbfReadOptions.LeaveOpen"/> controls both caller streams, including failed-open cleanup.</remarks>
    public static DbfDataReader Open(Stream stream, DbfReadOptions? options = null, Stream? memoStream = null,
        CancellationToken cancellationToken = default)
    {
        DbfReadOptions snapshot = (options ?? new DbfReadOptions()).Snapshot();
        ValidateStreams(stream, memoStream);
        return OpenCore(stream, memoStream, snapshot, !snapshot.LeaveOpen, cancellationToken);
    }

    /// <summary>Asynchronously opens a table file; subsequent <see cref="ReadAsync"/> calls use asynchronous row I/O.</summary>
    public static async Task<DbfDataReader> OpenAsync(string path, DbfReadOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(path)) throw new ArgumentException("A DBF path is required.", nameof(path));
        cancellationToken.ThrowIfCancellationRequested();
        DbfReadOptions snapshot = (options ?? new DbfReadOptions()).Snapshot();
        var table = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous | FileOptions.SequentialScan);
        DbfDataReader? reader = null;
        try
        {
            reader = await OpenCoreAsync(table, null, snapshot, ownsStreams: true, cancellationToken).ConfigureAwait(false);
            reader.OpenMemoFile(path);
            return reader;
        }
        catch { if (reader != null) reader.Dispose(); else table.Dispose(); throw; }
    }

    /// <summary>Asynchronously reads the header from caller streams. Memo value access remains synchronous and bounded.</summary>
    public static Task<DbfDataReader> OpenAsync(Stream stream, DbfReadOptions? options = null, Stream? memoStream = null,
        CancellationToken cancellationToken = default)
    {
        DbfReadOptions snapshot = (options ?? new DbfReadOptions()).Snapshot();
        ValidateStreams(stream, memoStream);
        return OpenCoreAsync(stream, memoStream, snapshot, !snapshot.LeaveOpen, cancellationToken);
    }

    private static void ValidateStreams(Stream stream, Stream? memoStream)
    {
        if (stream == null) throw new ArgumentNullException(nameof(stream));
        if (!stream.CanRead) throw new ArgumentException("The DBF stream must be readable.", nameof(stream));
        if (ReferenceEquals(stream, memoStream)) throw new ArgumentException("Table and memo streams must be distinct.", nameof(memoStream));
        if (memoStream != null && (!memoStream.CanRead || !memoStream.CanSeek))
            throw new ArgumentException("The memo stream must be readable and seekable.", nameof(memoStream));
    }

    private static long CheckTableSize(Stream stream, DbfReadOptions options)
    {
        if (!stream.CanSeek) return -1;
        long size = stream.Length - stream.Position;
        if (size < 33 || size > options.MaxInputBytes) throw new InvalidDataException("DBF source is truncated or exceeds MaxInputBytes.");
        return size;
    }

    private static DbfDataReader OpenCore(Stream stream, Stream? memo, DbfReadOptions options, bool ownsStreams, CancellationToken token)
    {
        try
        {
            token.ThrowIfCancellationRequested();
            long size = CheckTableSize(stream, options);
            var fixedHeader = new byte[32];
            DbfIO.ReadExactly(stream, fixedHeader, 0, fixedHeader.Length, token);
            var headerBytes = new byte[DbfIO.GetHeaderLength(fixedHeader, options)];
            Array.Copy(fixedHeader, headerBytes, fixedHeader.Length);
            DbfIO.ReadExactly(stream, headerBytes, 32, headerBytes.Length - 32, token);
            return CompleteOpen(stream, memo, options, ownsStreams, token, size, headerBytes);
        }
        catch { DisposeFailedOpen(stream, memo, ownsStreams); throw; }
    }

    private static async Task<DbfDataReader> OpenCoreAsync(Stream stream, Stream? memo, DbfReadOptions options, bool ownsStreams, CancellationToken token)
    {
        try
        {
            token.ThrowIfCancellationRequested();
            long size = CheckTableSize(stream, options);
            var fixedHeader = new byte[32];
            await DbfIO.ReadExactlyAsync(stream, fixedHeader, 0, fixedHeader.Length, token).ConfigureAwait(false);
            var headerBytes = new byte[DbfIO.GetHeaderLength(fixedHeader, options)];
            Array.Copy(fixedHeader, headerBytes, fixedHeader.Length);
            await DbfIO.ReadExactlyAsync(stream, headerBytes, 32, headerBytes.Length - 32, token).ConfigureAwait(false);
            return CompleteOpen(stream, memo, options, ownsStreams, token, size, headerBytes);
        }
        catch { DisposeFailedOpen(stream, memo, ownsStreams); throw; }
    }

    private static DbfDataReader CompleteOpen(Stream stream, Stream? memo, DbfReadOptions options, bool ownsStreams,
        CancellationToken token, long size, byte[] headerBytes)
    {
        DbfHeaderReader header = DbfHeaderReader.Parse(headerBytes, options);
        long expected = header.Schema.HeaderLength + header.Schema.RecordCount * header.Schema.RecordLength;
        if (size >= 0 && size < expected) throw new InvalidDataException("DBF source does not contain its declared records.");
        var reader = new DbfDataReader(stream, options, header, ownsStreams, token);
        if (memo != null) reader.AttachMemo(memo, ownsStreams);
        return reader;
    }

    private static void DisposeFailedOpen(Stream stream, Stream? memo, bool ownsStreams)
    {
        if (!ownsStreams) return;
        try { memo?.Dispose(); }
        finally { stream.Dispose(); }
    }

    private void AttachMemo(Stream memo, bool ownsStream)
    {
        _memo = new DbfMemoReader(memo, _header.UsesFpt, _options, _cancellationToken);
        _ownsMemo = ownsStream;
    }

    private void OpenMemoFile(string tablePath)
    {
        if (!Schema.Columns.Any(static column => column.IsMemo)) return;
        string extension = _header.UsesFpt ? ".fpt" : ".dbt";
        string sidecar = Path.ChangeExtension(tablePath, extension);
        if (!File.Exists(sidecar)) sidecar = Path.ChangeExtension(tablePath, extension.ToUpperInvariant());
        if (!File.Exists(sidecar)) return;
        var stream = new FileStream(sidecar, FileMode.Open, FileAccess.Read, FileShare.Read);
        try { AttachMemo(stream, ownsStream: true); }
        catch { stream.Dispose(); throw; }
    }
}
