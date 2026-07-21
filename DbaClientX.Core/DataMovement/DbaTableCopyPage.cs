using System.Data;

namespace DBAClientX.DataMovement;

/// <summary>A source page and the opaque provider token needed to request the following page.</summary>
public sealed class DbaTableCopyPage : IDisposable
{
    /// <summary>Creates a page with an optional opaque token for the following page.</summary>
    public DbaTableCopyPage(DataTable data, string? continuationToken = null)
    {
        Data = data ?? throw new ArgumentNullException(nameof(data));
        if (continuationToken != null && continuationToken.Length == 0)
        {
            throw new ArgumentException("Continuation token cannot be empty.", nameof(continuationToken));
        }

        ContinuationToken = continuationToken;
    }

    /// <summary>Rows and schema returned by the source.</summary>
    public DataTable Data { get; }

    /// <summary>Opaque token for the next page, or null when the stream is complete.</summary>
    public string? ContinuationToken { get; }

    /// <summary>Creates a page for a legacy offset-backed provider.</summary>
    public static DbaTableCopyPage FromOffset(DataTable data, long? nextOffset)
        => new(data, nextOffset.HasValue ? DbaOffsetContinuationToken.Encode(nextOffset.Value) : null);

    /// <inheritdoc />
    public void Dispose() => Data.Dispose();
}
