using System.Collections.Generic;
using System.Globalization;

namespace DBAClientX.QueryPlans;

internal static partial class SqlQueryLevels
{
    /// <summary>Counts skipped rows as reads; unresolved bounds cannot establish a narrow read.</summary>
    private static bool LimitReadsFewRows(IReadOnlyList<SqlToken> tokens, int position, int end, double? maximumRows)
    {
        if (!ReadNonNegativeInteger(tokens, position, end, out double first)) return false;
        double count = first;
        double offset = 0;
        position++;
        if (position < end && tokens[position].Text == ",")
        {
            offset = first;
            if (!ReadNonNegativeInteger(tokens, position + 1, end, out count)) return false;
            position += 2;
        }
        else if (position < end && IsWord(tokens[position], "OFFSET"))
        {
            if (!ReadNonNegativeInteger(tokens, position + 1, end, out offset)) return false;
            position += 2;
        }

        // A numeric prefix does not bound an expression such as LIMIT 10 + @more.
        if (position < end && tokens[position].Kind is not (SqlTokenKind.Semicolon or SqlTokenKind.CloseParenthesis)) return false;

        return maximumRows.HasValue ? count + offset <= maximumRows.Value : offset == 0;
    }

    private static bool ReadNonNegativeInteger(IReadOnlyList<SqlToken> tokens, int position, int end, out double value)
    {
        value = 0;
        if (position >= end || tokens[position].Kind != SqlTokenKind.Number ||
            !long.TryParse(tokens[position].Text, NumberStyles.None, CultureInfo.InvariantCulture, out long integer)) return false;
        value = integer;
        return true;
    }
}
