namespace DBAClientX;

/// <summary>
/// Describes the result of an SQLite integrity check.
/// </summary>
public sealed class SqliteIntegrityCheckResult
{
    /// <summary>Gets a value indicating whether SQLite reported a healthy database.</summary>
    public bool IsHealthy { get; set; }

    /// <summary>Gets a value indicating whether the full integrity check was requested.</summary>
    public bool IsFullCheck { get; set; }

    /// <summary>Gets the issues returned by SQLite.</summary>
    public IReadOnlyList<string> Issues { get; set; } = Array.Empty<string>();

    /// <summary>Gets the elapsed check duration.</summary>
    public TimeSpan Elapsed { get; set; }

    /// <summary>
    /// Gets how many pages the check read from the file because they were not in its page cache. Far more than the
    /// database's page count means the cache was too small and the check re-read pages.
    /// </summary>
    public long PagesRead { get; set; }
}
