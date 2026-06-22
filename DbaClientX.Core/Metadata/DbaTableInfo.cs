namespace DBAClientX.Metadata;

/// <summary>
/// Identifies a tabular database object such as a table or view.
/// </summary>
public sealed record DbaTableInfo(string Schema, string Name, DbaTableKind Kind)
{
    /// <summary>Provider-specific object owner when different from <see cref="Schema"/>.</summary>
    public string? Owner { get; init; }
}
