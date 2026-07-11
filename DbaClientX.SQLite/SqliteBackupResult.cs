namespace DBAClientX;

/// <summary>
/// Describes a completed SQLite online backup.
/// </summary>
public sealed class SqliteBackupResult
{
    /// <summary>Gets the normalized source database path.</summary>
    public string SourceDatabase { get; set; } = string.Empty;

    /// <summary>Gets the normalized destination database path.</summary>
    public string DestinationDatabase { get; set; } = string.Empty;

    /// <summary>Gets the final number of pages copied.</summary>
    public int CopiedPages { get; set; }

    /// <summary>Gets the final destination length in bytes.</summary>
    public long DestinationLengthBytes { get; set; }

    /// <summary>Gets the elapsed backup duration.</summary>
    public TimeSpan Elapsed { get; set; }
}
