using System.Collections.Generic;
using System.Text;

namespace DBAClientX.Payload;

/// <summary>
/// Minimal JSON/Text serialization helpers without external dependencies.
/// Hosts can replace this with their favorite serializer if needed.
/// </summary>
public static class DbPayloadSerializer
{
    /// <summary>
    /// Serializes a sequence of items to a JSON array using simple escaping and invariant formatting.
    /// </summary>
    /// <param name="items">The items to serialize.</param>
    /// <returns>JSON array string.</returns>
    public static string ToJsonArray(IEnumerable<object> items)
    {
        var sb = new StringBuilder();
        sb.Append('[');
        var first = true;
        foreach (var it in items)
        {
            if (!first) sb.Append(',');
            first = false;
            SerializeValue(sb, it);
        }
        sb.Append(']');
        return sb.ToString();
    }

    /// <summary>
    /// Converts a sequence of items to newline-separated text using <c>ToString()</c> on each item.
    /// </summary>
    /// <param name="items">The items to serialize.</param>
    /// <returns>Newline-separated text.</returns>
    public static string ToTextLines(IEnumerable<object> items)
    {
        var sb = new StringBuilder();
        var first = true;
        foreach (var it in items)
        {
            if (!first) sb.Append('\n');
            first = false;
            sb.Append(it?.ToString());
        }
        return sb.ToString();
    }

    private static void SerializeValue(StringBuilder sb, object? value)
    {
        if (value is null) { sb.Append("null"); return; }
        switch (value)
        {
            case string s:
                AppendEscapedString(sb, s);
                break;
            case bool b:
                sb.Append(b ? "true" : "false");
                break;
            case byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal:
                sb.Append(System.Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            default:
                AppendEscapedString(sb, value.ToString() ?? string.Empty);
                break;
        }
    }

    private static void AppendEscapedString(StringBuilder sb, string s)
    {
        sb.Append('"');
        for (int i = 0; i < s.Length; i++)
        {
            char c = s[i];
            switch (c)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                default:
                    if (c < 0x20)
                    {
                        sb.Append("\\u");
                        sb.Append(((int)c).ToString("x4"));
                    }
                    else
                    {
                        sb.Append(c);
                    }
                    break;
            }
        }
        sb.Append('"');
    }
}
