namespace DBAClientX.DataMovement;

/// <summary>
/// Represents a calendar interval as a signed number of whole months.
/// </summary>
/// <remarks>
/// This type preserves database year-to-month intervals without treating them as an ordinary
/// integer or approximating months as a fixed number of days.
/// </remarks>
public readonly struct DbaYearMonthInterval : IEquatable<DbaYearMonthInterval>
{
    /// <summary>Initializes an interval with the specified signed number of months.</summary>
    /// <param name="totalMonths">The signed total number of months.</param>
    public DbaYearMonthInterval(long totalMonths) => TotalMonths = totalMonths;

    /// <summary>Gets the signed total number of months in the interval.</summary>
    public long TotalMonths { get; }

    /// <inheritdoc />
    public bool Equals(DbaYearMonthInterval other) => TotalMonths == other.TotalMonths;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is DbaYearMonthInterval other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => TotalMonths.GetHashCode();

    /// <inheritdoc />
    public override string ToString() => TotalMonths.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>Determines whether two intervals contain the same number of months.</summary>
    public static bool operator ==(DbaYearMonthInterval left, DbaYearMonthInterval right) => left.Equals(right);

    /// <summary>Determines whether two intervals contain a different number of months.</summary>
    public static bool operator !=(DbaYearMonthInterval left, DbaYearMonthInterval right) => !left.Equals(right);
}
