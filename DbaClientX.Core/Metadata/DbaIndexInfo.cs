namespace DBAClientX.Metadata;

/// <summary>
/// Describes an index column. Multi-column indexes return one row per indexed column.
/// </summary>
public sealed record DbaIndexInfo(string Schema, string Table, string Name)
{
    /// <summary>Provider-specific index type or access method when available.</summary>
    public string? IndexType { get; init; }

    /// <summary>Indicates whether the index is unique.</summary>
    public bool IsUnique { get; init; }

    /// <summary>Indicates whether the index backs a primary key.</summary>
    public bool IsPrimaryKey { get; init; }

    /// <summary>Indexed column name when the provider exposes a direct column mapping.</summary>
    public string? Column { get; init; }

    /// <summary>Indexed expression text when the index key is not a direct column reference.</summary>
    public string? Expression { get; init; }

    /// <summary>One-based ordinal position of the column inside the index.</summary>
    public int Ordinal { get; init; }

    /// <summary>Indicates descending sort order when the provider exposes it.</summary>
    public bool? IsDescending { get; init; }

    /// <summary>Indicates whether the column is included but not part of the index key.</summary>
    public bool? IsIncluded { get; init; }

    /// <summary>Indicates whether the provider reports the index as visible to query optimization.</summary>
    public bool? IsVisible { get; init; }

    /// <summary>Provider-specific prefix length for partial-width index keys.</summary>
    public int? PrefixLength { get; init; }

    /// <summary>Provider-specific filter or partial-index predicate when available.</summary>
    public string? FilterDefinition { get; init; }
}
