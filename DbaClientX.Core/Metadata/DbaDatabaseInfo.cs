namespace DBAClientX.Metadata;

/// <summary>
/// Describes a database or catalog visible to a provider connection.
/// </summary>
public sealed record DbaDatabaseInfo(string Name)
{
    /// <summary>Provider-specific owner, schema owner, or current user when available.</summary>
    public string? Owner { get; init; }

    /// <summary>Provider-specific collation or encoding when available.</summary>
    public string? Collation { get; init; }

    /// <summary>Indicates whether the database is provider/system-owned when the provider exposes that information.</summary>
    public bool? IsSystem { get; init; }
}
