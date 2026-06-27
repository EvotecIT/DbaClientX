namespace DBAClientX.DataMovement;

/// <summary>
/// Describes simple destination column conversions applied while copying table pages.
/// </summary>
public enum DbaTableCopyColumnType
{
    /// <summary>Leave the source value type unchanged.</summary>
    None = 0,

    /// <summary>Convert common numeric/string values to <see cref="bool"/>.</summary>
    Boolean,

    /// <summary>Convert values to <see cref="int"/>.</summary>
    Int32,

    /// <summary>Convert values to <see cref="long"/>.</summary>
    Int64,

    /// <summary>Convert values to <see cref="decimal"/>.</summary>
    Decimal,

    /// <summary>Convert values to <see cref="string"/>.</summary>
    String,

    /// <summary>Convert values to <see cref="DateTime"/>.</summary>
    DateTime
}
