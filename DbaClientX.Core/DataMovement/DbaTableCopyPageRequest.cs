using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>Describes a provider-owned continuation page request for a table copy.</summary>
public sealed record DbaTableCopyPageRequest
{
    /// <summary>Creates a continuation-token page request. A null token requests the first page.</summary>
    public DbaTableCopyPageRequest(DbaTableCopyDefinition definition, string? continuationToken, int pageSize)
    {
        Definition = definition ?? throw new ArgumentNullException(nameof(definition));
        if (continuationToken != null && continuationToken.Length == 0)
        {
            throw new ArgumentException("Continuation token cannot be empty.", nameof(continuationToken));
        }

        if (pageSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(pageSize), "Page size must be greater than zero.");
        }

        ContinuationToken = continuationToken;
        PageSize = pageSize;
    }

    /// <summary>Compatibility constructor for offset-based sources.</summary>
    [Obsolete("Implement provider-owned continuation tokens. This offset constructor remains only for migration.")]
    public DbaTableCopyPageRequest(DbaTableCopyDefinition definition, long offset, int pageSize)
        : this(definition, DbaOffsetContinuationToken.Encode(offset), pageSize)
    {
    }

    /// <summary>Copy definition describing the source to read.</summary>
    public DbaTableCopyDefinition Definition { get; }

    /// <summary>Opaque provider token returned by the preceding page, or null for the first page.</summary>
    public string? ContinuationToken { get; }

    /// <summary>Maximum number of rows requested for this page.</summary>
    public int PageSize { get; }

    /// <summary>Decoded offset for legacy offset-backed sources.</summary>
    [Obsolete("ContinuationToken is the canonical paging contract.")]
    public long Offset => DbaOffsetContinuationToken.Decode(ContinuationToken);
}

internal static class DbaOffsetContinuationToken
{
    private const string Prefix = "dbax-offset:";

    internal static string? Encode(long offset)
    {
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        return offset == 0 ? null : Prefix + offset.ToString(CultureInfo.InvariantCulture);
    }

    internal static long Decode(string? continuationToken)
    {
        if (continuationToken == null) return 0;
        if (!continuationToken.StartsWith(Prefix, StringComparison.Ordinal) ||
            !long.TryParse(continuationToken.Substring(Prefix.Length), NumberStyles.None, CultureInfo.InvariantCulture, out long offset) ||
            offset <= 0)
        {
            throw new ArgumentException("The continuation token was not issued by the DbaClientX offset paging adapter.", nameof(continuationToken));
        }

        return offset;
    }
}
