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

    /// <summary>Indicates whether the provider reports the column as identity or auto-incrementing.</summary>
    public bool? IsIdentity { get; init; }

    /// <summary>Provider-specific identity generation strategy, such as IDENTITY, AUTO_INCREMENT, ALWAYS, or BY DEFAULT.</summary>
    public string? IdentityGeneration { get; init; }

    /// <summary>Provider-specific generated or computed column expression when available.</summary>
    public string? GeneratedExpression { get; init; }

    /// <summary>Provider-specific generated column kind, such as COMPUTED, STORED, or VIRTUAL.</summary>
    public string? GeneratedKind { get; init; }
}
