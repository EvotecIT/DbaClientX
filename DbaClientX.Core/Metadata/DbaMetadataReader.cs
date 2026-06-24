using System;
using System.Data;
using System.Globalization;

namespace DBAClientX.Metadata;

/// <summary>
/// Provides small conversion helpers for provider metadata readers.
/// </summary>
public static class DbaMetadataReader
{
    /// <summary>Reads a required string field.</summary>
    public static string GetString(IDataRecord record, string name)
        => Convert.ToString(GetValue(record, name), CultureInfo.InvariantCulture) ?? string.Empty;

    /// <summary>Reads an optional string field.</summary>
    public static string? GetNullableString(IDataRecord record, string name)
    {
        var value = GetValue(record, name);
        return value == null || value == DBNull.Value
            ? null
            : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>Reads a required integer field.</summary>
    public static int GetInt32(IDataRecord record, string name)
        => Convert.ToInt32(GetValue(record, name), CultureInfo.InvariantCulture);

    /// <summary>Reads an optional integer field.</summary>
    public static int? GetNullableInt32(IDataRecord record, string name)
    {
        var value = GetValue(record, name);
        return value == null || value == DBNull.Value
            ? null
            : Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    /// <summary>Reads an optional long field.</summary>
    public static long? GetNullableInt64(IDataRecord record, string name)
    {
        var value = GetValue(record, name);
        return value == null || value == DBNull.Value
            ? null
            : Convert.ToInt64(value, CultureInfo.InvariantCulture);
    }

    /// <summary>Reads a required Boolean field.</summary>
    public static bool GetBoolean(IDataRecord record, string name)
        => ConvertToBoolean(GetValue(record, name)) ?? false;

    /// <summary>Reads an optional Boolean field.</summary>
    public static bool? GetNullableBoolean(IDataRecord record, string name)
        => ConvertToBoolean(GetValue(record, name));

    private static object? GetValue(IDataRecord record, string name)
    {
        var ordinal = record.GetOrdinal(name);
        return record.IsDBNull(ordinal) ? null : record.GetValue(ordinal);
    }

    private static bool? ConvertToBoolean(object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            return null;
        }

        if (value is bool boolean)
        {
            return boolean;
        }

        if (value is string text)
        {
            if (bool.TryParse(text, out var parsed))
            {
                return parsed;
            }

            if (string.Equals(text, "YES", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(text, "Y", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(text, "1", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (string.Equals(text, "NO", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(text, "N", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(text, "0", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return Convert.ToInt32(value, CultureInfo.InvariantCulture) != 0;
    }
}
