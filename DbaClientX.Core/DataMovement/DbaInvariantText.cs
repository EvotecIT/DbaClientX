using System.Globalization;

namespace DBAClientX.DataMovement;

/// <summary>
/// Culture-independent text for values moved between typed and character columns. Numbers use the invariant culture
/// and temporal values use the ISO form SQL Server produces for CAST(... AS nvarchar), so text never depends on the
/// session culture (pl-PL wrote 0.25 as "0,25").
/// </summary>
internal static class DbaInvariantText
{
    // Parsing rejects thousands separators: "14,5" must not become 145.
    private const NumberStyles DecimalStyle = NumberStyles.AllowLeadingWhite | NumberStyles.AllowTrailingWhite | NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint;
    private const NumberStyles FloatStyle = NumberStyles.Float;
    private const NumberStyles IntegerStyle = NumberStyles.Integer;

    /// <summary>Formats numeric and temporal values; returns false for other values.</summary>
    internal static bool TryFormat(object value, out string text)
    {
        switch (value)
        {
            case DateTime dateTime:
                text = dateTime.ToString("yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                return true;
            case DateTimeOffset dateTimeOffset:
                text = dateTimeOffset.ToString("yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture);
                return true;
            case TimeSpan time when time >= TimeSpan.Zero && time.Ticks < TimeSpan.TicksPerDay:
                text = time.ToString(@"hh\:mm\:ss\.fffffff", CultureInfo.InvariantCulture);
                return true;
            case TimeSpan time:
                text = time.ToString("c", CultureInfo.InvariantCulture);
                return true;
            // Shortest round-trip text: CAST(float AS nvarchar) keeps only six digits, which loses data.
            case double number:
                text = number.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case float number:
                text = number.ToString("R", CultureInfo.InvariantCulture);
                return true;
            case decimal or byte or sbyte or short or ushort or int or uint or long or ulong:
                text = ((IFormattable)value).ToString(null, CultureInfo.InvariantCulture);
                return true;
#if NET6_0_OR_GREATER
            case DateOnly date:
                text = date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                return true;
            case TimeOnly time:
                text = time.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture);
                return true;
#endif
            default:
                text = string.Empty;
                return false;
        }
    }

    /// <summary>
    /// Parses text for a numeric or temporal target as SQL Server would (invariant), then with the session culture,
    /// so locale-formatted text that is not valid invariant text keeps loading. Returns false when neither applies.
    /// </summary>
    internal static bool TryParse(string text, Type target, out object value)
    {
        if (TryParse(text, target, CultureInfo.InvariantCulture, out value)) return true;
        return !ReferenceEquals(CultureInfo.CurrentCulture, CultureInfo.InvariantCulture) &&
               TryParse(text, target, CultureInfo.CurrentCulture, out value);
    }

    /// <summary>Returns whether <paramref name="target"/> has culture-sensitive text that <see cref="TryParse(string, Type, out object)"/> handles.</summary>
    internal static bool IsParseTarget(Type target)
        => target == typeof(decimal) || target == typeof(double) || target == typeof(float) ||
           target == typeof(long) || target == typeof(int) || target == typeof(short) || target == typeof(byte) ||
           target == typeof(DateTime) || target == typeof(DateTimeOffset) || target == typeof(TimeSpan);

    private static bool TryParse(string text, Type target, CultureInfo culture, out object value)
    {
        bool parsed;
        if (target == typeof(decimal)) { parsed = decimal.TryParse(text, DecimalStyle, culture, out decimal result); value = result; }
        else if (target == typeof(double)) { parsed = double.TryParse(text, FloatStyle, culture, out double result); value = result; }
        else if (target == typeof(float)) { parsed = float.TryParse(text, FloatStyle, culture, out float result); value = result; }
        else if (target == typeof(long)) { parsed = long.TryParse(text, IntegerStyle, culture, out long result); value = result; }
        else if (target == typeof(int)) { parsed = int.TryParse(text, IntegerStyle, culture, out int result); value = result; }
        else if (target == typeof(short)) { parsed = short.TryParse(text, IntegerStyle, culture, out short result); value = result; }
        else if (target == typeof(byte)) { parsed = byte.TryParse(text, IntegerStyle, culture, out byte result); value = result; }
        else if (target == typeof(DateTime)) { parsed = TryParseDateTime(text, culture, out DateTime result); value = result; }
        else if (target == typeof(DateTimeOffset)) { parsed = DateTimeOffset.TryParse(text, culture, DateTimeStyles.AllowWhiteSpaces, out DateTimeOffset result); value = result; }
        else if (target == typeof(TimeSpan)) { parsed = TimeSpan.TryParse(text.Trim(), culture, out TimeSpan result); value = result; }
        else { parsed = false; value = text; }

        if (!parsed) value = text;
        return parsed;
    }

    // ISO text with Z or an offset keeps the written date and time, as CAST to datetime2 does. DateTime.TryParse
    // converted it to the local time zone, so a UTC value exported as "...Z" loaded shifted by the local offset.
    private static bool TryParseDateTime(string text, CultureInfo culture, out DateTime value)
    {
        if (DateTimeOffset.TryParse(text, culture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeUniversal, out DateTimeOffset withOffset))
        {
            value = withOffset.DateTime;
            return true;
        }

        return DateTime.TryParse(text, culture, DateTimeStyles.AllowWhiteSpaces, out value);
    }
}
