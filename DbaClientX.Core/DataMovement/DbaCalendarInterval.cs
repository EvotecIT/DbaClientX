using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>
/// Represents a calendar interval without approximating months as a fixed number of days.
/// </summary>
/// <remarks>
/// The three components intentionally mirror database interval semantics: whole months, whole days,
/// and the remaining sub-day time measured in microseconds.
/// </remarks>
public readonly struct DbaCalendarInterval : IEquatable<DbaCalendarInterval>
{
    /// <summary>Initializes a calendar interval.</summary>
    public DbaCalendarInterval(int months, int days, long microseconds)
    {
        Months = months;
        Days = days;
        Microseconds = microseconds;
    }

    /// <summary>Gets the signed whole-month component.</summary>
    public int Months { get; }

    /// <summary>Gets the signed whole-day component.</summary>
    public int Days { get; }

    /// <summary>Gets the signed sub-day component in microseconds.</summary>
    public long Microseconds { get; }

    /// <inheritdoc />
    public bool Equals(DbaCalendarInterval other)
        => Months == other.Months && Days == other.Days && Microseconds == other.Microseconds;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is DbaCalendarInterval other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = Months;
            hash = (hash * 397) ^ Days;
            hash = (hash * 397) ^ Microseconds.GetHashCode();
            return hash;
        }
    }

    /// <inheritdoc />
    public override string ToString()
        => string.Format(
            CultureInfo.InvariantCulture,
            "months={0};days={1};microseconds={2}",
            Months,
            Days,
            Microseconds);

    /// <summary>Determines whether two intervals contain identical components.</summary>
    public static bool operator ==(DbaCalendarInterval left, DbaCalendarInterval right) => left.Equals(right);

    /// <summary>Determines whether two intervals contain different components.</summary>
    public static bool operator !=(DbaCalendarInterval left, DbaCalendarInterval right) => !left.Equals(right);
}
