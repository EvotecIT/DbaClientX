namespace DBAClientX;

/// <summary>
/// Describes progress reported by an incremental SQLite online backup.
/// </summary>
public sealed class SqliteBackupProgress
{
    /// <summary>Gets the number of source pages reported by SQLite.</summary>
    public int TotalPages { get; set; }

    /// <summary>Gets the number of pages copied into the destination.</summary>
    public int CopiedPages { get; set; }

    /// <summary>Gets the number of pages remaining.</summary>
    public int RemainingPages { get; set; }

    /// <summary>Gets the completion percentage when the total page count is known.</summary>
    public double PercentComplete { get; set; }

    /// <summary>Gets the elapsed backup duration.</summary>
    public TimeSpan Elapsed { get; set; }
}
