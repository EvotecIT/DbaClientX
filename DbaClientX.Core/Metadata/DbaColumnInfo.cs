namespace DBAClientX.Metadata;

/// <summary>
/// Describes a column on a provider-neutral table or view result.
/// </summary>
public sealed record DbaColumnInfo(string Schema, string Table, string Name, string DataType)
{
    /// <summary>One-based ordinal position in the table or view.</summary>
    public int Ordinal { get; init; }

    /// <summary>Indicates whether the provider reports the column as nullable.</summary>
    public bool? IsNullable { get; init; }

    /// <summary>Maximum character or binary length when available.</summary>
    public long? MaxLength { get; init; }

    /// <summary>Numeric precision when available.</summary>
    public int? Precision { get; init; }

    /// <summary>Numeric scale when available.</summary>
    public int? Scale { get; init; }

    /// <summary>Provider-specific default expression when available.</summary>
    public string? DefaultExpression { get; init; }
}
