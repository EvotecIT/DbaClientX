namespace DBAClientX.Metadata;

/// <summary>
/// Describes one column mapping in a foreign key relationship.
/// </summary>
public sealed record DbaForeignKeyInfo(
    string Schema,
    string Table,
    string Name,
    string Column,
    string ReferencedSchema,
    string ReferencedTable,
    string ReferencedColumn)
{
    /// <summary>One-based ordinal position of the column inside the foreign key.</summary>
    public int Ordinal { get; init; }

    /// <summary>Provider-specific update action when available.</summary>
    public string? UpdateRule { get; init; }

    /// <summary>Provider-specific delete action when available.</summary>
    public string? DeleteRule { get; init; }

    /// <summary>Indicates whether the provider reports the constraint as enabled.</summary>
    public bool? IsEnabled { get; init; }

    /// <summary>Indicates whether the provider reports the constraint as validated or trusted.</summary>
    public bool? IsTrusted { get; init; }
}
