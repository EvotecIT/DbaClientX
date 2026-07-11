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
}
