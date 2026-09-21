using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>
/// Represents an exact decimal number whose precision or scale may exceed <see cref="decimal"/>.
/// </summary>
/// <remarks>
/// The value is stored in a provider-neutral canonical form so it can be used in continuation
/// tokens and content verification without narrowing it to a CLR numeric type. A destination
/// provider that cannot represent the required precision needs an explicit column conversion.
/// </remarks>
public readonly struct DbaArbitraryDecimal : IEquatable<DbaArbitraryDecimal>
{
    private const int MaximumCanonicalLength = 4096;
    private readonly string? _canonicalValue;

    /// <summary>Initializes an exact decimal value from invariant decimal or scientific notation.</summary>
    /// <param name="value">The invariant numeric representation.</param>
    public DbaArbitraryDecimal(string value)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        _canonicalValue = Normalize(value);
    }

    /// <summary>Gets the exact invariant value without redundant leading or trailing zeroes.</summary>
    public string CanonicalValue => _canonicalValue ?? "0";

    /// <inheritdoc />
    public bool Equals(DbaArbitraryDecimal other)
        => string.Equals(CanonicalValue, other.CanonicalValue, StringComparison.Ordinal);

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is DbaArbitraryDecimal other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(CanonicalValue);

    /// <inheritdoc />
    public override string ToString() => CanonicalValue;

    /// <summary>Determines whether two values represent the same exact decimal number.</summary>
    public static bool operator ==(DbaArbitraryDecimal left, DbaArbitraryDecimal right) => left.Equals(right);

    /// <summary>Determines whether two values represent different exact decimal numbers.</summary>
    public static bool operator !=(DbaArbitraryDecimal left, DbaArbitraryDecimal right) => !left.Equals(right);

    private static string Normalize(string value)
    {
        if (value.Length == 0 || !string.Equals(value, value.Trim(), StringComparison.Ordinal))
            throw new FormatException("An exact decimal value cannot be empty or contain surrounding whitespace.");

        var index = 0;
        bool negative = false;
        if (value[index] is '+' or '-')
        {
            negative = value[index++] == '-';
            if (index == value.Length) throw new FormatException("Invalid exact decimal value.");
        }

        var digits = new StringBuilder(value.Length);
        var fractionDigits = 0;
        var seenDigit = false;
        while (index < value.Length && value[index] is >= '0' and <= '9')
        {
            digits.Append(value[index++]);
            seenDigit = true;
        }
        if (index < value.Length && value[index] == '.')
        {
            index++;
            int fractionStart = digits.Length;
            while (index < value.Length && value[index] is >= '0' and <= '9')
            {
                digits.Append(value[index++]);
                seenDigit = true;
            }
            fractionDigits = digits.Length - fractionStart;
        }
        if (!seenDigit) throw new FormatException("Invalid exact decimal value.");

        var exponent = 0;
        if (index < value.Length && value[index] is 'e' or 'E')
        {
            index++;
            int exponentStart = index;
            if (index < value.Length && value[index] is '+' or '-') index++;
            int exponentDigits = index;
            while (index < value.Length && value[index] is >= '0' and <= '9') index++;
            if (exponentDigits == index || !int.TryParse(value.Substring(exponentStart, index - exponentStart), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out exponent))
                throw new FormatException("Invalid exact decimal exponent.");
        }
        if (index != value.Length) throw new FormatException("Invalid exact decimal value.");

        int firstNonZero = 0;
        while (firstNonZero < digits.Length && digits[firstNonZero] == '0') firstNonZero++;
        if (firstNonZero == digits.Length) return "0";
        digits.Remove(0, firstNonZero);

        long scaleValue = (long)fractionDigits - exponent;
        while (digits.Length > 1 && digits[digits.Length - 1] == '0')
        {
            digits.Length--;
            scaleValue--;
        }
        if (scaleValue is < -MaximumCanonicalLength or > MaximumCanonicalLength ||
            digits.Length > MaximumCanonicalLength)
            throw new FormatException("The exact decimal value exceeds the supported canonical length.");

        int scale = (int)scaleValue;
        string magnitude;
        if (scale <= 0)
        {
            magnitude = digits + new string('0', -scale);
        }
        else if (scale >= digits.Length)
        {
            magnitude = "0." + new string('0', scale - digits.Length) + digits;
        }
        else
        {
            digits.Insert(digits.Length - scale, '.');
            magnitude = digits.ToString();
        }
        if (magnitude.Length > MaximumCanonicalLength)
            throw new FormatException("The exact decimal value exceeds the supported canonical length.");
        string canonical = negative ? "-" + magnitude : magnitude;
        if (canonical.Length > MaximumCanonicalLength)
            throw new FormatException("The exact decimal value exceeds the supported canonical length.");
        return canonical;
    }
}
