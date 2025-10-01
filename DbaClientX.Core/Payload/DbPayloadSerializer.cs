using System.Collections.Generic;
using System.Text;

namespace DBAClientX.Payload;

/// <summary>
/// Minimal JSON/Text serialization helpers without external dependencies.
/// Hosts can replace this with their favorite serializer if needed.
/// </summary>
public static class DbPayloadSerializer
{
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
                sb.Append('"').Append(s.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
                break;
            case bool b:
                sb.Append(b ? "true" : "false");
                break;
            case byte or sbyte or short or ushort or int or uint or long or ulong or float or double or decimal:
                sb.Append(System.Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture));
                break;
            default:
                sb.Append('"').Append(value.ToString()?.Replace("\\", "\\\\").Replace("\"", "\\\"")).Append('"');
                break;
        }
    }
}

