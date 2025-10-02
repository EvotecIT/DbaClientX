using System;
using System.Collections.Generic;

namespace DBAClientX.Mapping;

/// <summary>
/// Provides configuration for mapping object properties to provider parameters.
/// </summary>
public sealed class DbParameterMapperOptions
{
    /// <summary>
    /// When true, enums are converted to string; otherwise to underlying integral type.
    /// Default: false (integral).
    /// </summary>
    public bool EnumsAsString { get; init; }

    /// <summary>
    /// Convert DateTimeOffset to UTC DateTime. Default: true.
    /// </summary>
    public bool DateTimeOffsetAsUtcDateTime { get; init; } = true;

    /// <summary>
    /// Optional custom converters by type.
    /// </summary>
    public Dictionary<Type, Func<object, object?>> Converters { get; } = new();
}

/// <summary>
/// Builds a dictionary mapping provider parameter names (e.g., "@UserName") to values
/// by reading properties from a typed item according to a logical-&gt;provider map.
/// </summary>
public static class DbParameterMapper
{
    /// <summary>
    /// Maps a single item to a provider parameter dictionary using the supplied map and options.
    /// </summary>
    /// <param name="item">Typed source item (POCO or dictionary).</param>
    /// <param name="map">Logical name to provider parameter map. Keys can be dotted paths (e.g., "User.Name"). Values are provider parameter names (e.g., "@UserName").</param>
    /// <param name="options">Conversion options (enum handling, DateTimeOffset conversion, custom converters).</param>
    /// <param name="ambient">Optional ambient values available to mappings when the item does not provide a value (e.g., RunId, TsUtc).</param>
    /// <returns>A new dictionary of provider parameters to values.</returns>
    public static IDictionary<string, object?> MapItem(
        object? item,
        IReadOnlyDictionary<string, string> map,
        DbParameterMapperOptions? options = null,
        IReadOnlyDictionary<string, object?>? ambient = null)
    {
        options ??= new DbParameterMapperOptions();
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var kv in map)
        {
            var logical = kv.Key;       // e.g. "UserName" or "User.Name"
            var providerParam = kv.Value; // e.g. "@UserName"

            if (ambient != null && ambient.TryGetValue(logical, out var ambientVal))
            {
                result[providerParam] = ConvertValue(ambientVal, options);
                continue;
            }

            if (item != null && DbPropertyAccessor.TryGetValue(item, logical, out var value))
            {
                result[providerParam] = ConvertValue(value, options);
                continue;
            }

            // Not found; map null
            result[providerParam] = null;
        }

        return result;
    }

    private static object? ConvertValue(object? value, DbParameterMapperOptions options)
    {
        if (value is null) return null;

        if (options.Converters.Count > 0)
        {
            if (options.Converters.TryGetValue(value.GetType(), out var conv))
            {
                return conv(value);
            }
        }

        if (value is DateTimeOffset dto && options.DateTimeOffsetAsUtcDateTime)
        {
            return dto.UtcDateTime;
        }
        if (value is Enum e)
        {
            return options.EnumsAsString ? e.ToString() : Convert.ChangeType(e, Enum.GetUnderlyingType(e.GetType()));
        }
        return value;
    }
}
