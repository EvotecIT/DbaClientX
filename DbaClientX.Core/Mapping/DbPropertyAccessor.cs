using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace DBAClientX.Mapping;

/// <summary>
/// Fast, case-insensitive property-path accessor for POCOs and dictionaries.
/// Supports dotted paths like "A.B.C" and falls back to dictionary lookups.
/// </summary>
public static class DbPropertyAccessor
{
    private static readonly object MissingValue = new();
    private static readonly ConcurrentDictionary<(Type, string), Func<object, object?>> _getterCache = new();

    /// <summary>
    /// Tries to resolve a value from an item using a dotted path. Case-insensitive.
    /// Handles POCOs (public properties), dictionaries, and list/array indexes in dotted paths.
    /// </summary>
    /// <param name="item">The source object (POCO or dictionary) to read from.</param>
    /// <param name="path">Dotted, case-insensitive path (e.g., "User.Name" or "Metadata.Tags.0").</param>
    /// <param name="value">When this method returns, contains the resolved value or null.</param>
    /// <returns>True when a value was resolved; otherwise false.</returns>
    [RequiresUnreferencedCode("Use the generic TryGetValue<T> overload when trimming so public properties can be preserved.")]
    public static bool TryGetValue(object? item, string path, out object? value)
        => TryGetValueCore(item, item?.GetType(), path, out value);

    /// <summary>
    /// Tries to resolve a value from a strongly typed item while preserving public properties for trimmed/AOT builds.
    /// </summary>
    /// <param name="item">The typed source object (POCO or dictionary) to read from.</param>
    /// <param name="path">Dotted, case-insensitive path (e.g., "User.Name" or "Metadata.Tags.0").</param>
    /// <param name="value">When this method returns, contains the resolved value or null.</param>
    /// <returns>True when a value was resolved; otherwise false.</returns>
    public static bool TryGetValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] T>(T? item, string path, out object? value)
        => TryGetValueCore(item, typeof(T), path, out value);

    private static bool TryGetValueCore(object? item, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] Type? type, string path, out object? value)
    {
        value = null;
        if (item is null || type is null || string.IsNullOrWhiteSpace(path)) return false;

        var key = (type, path);
        if (!_getterCache.TryGetValue(key, out var getter))
        {
            getter = _getterCache.GetOrAdd(key, BuildGetter(type, path));
        }

        if (getter is null) return false;
        value = getter(item);
        if (ReferenceEquals(value, MissingValue))
        {
            value = null;
            return false;
        }
        return true;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2067", Justification = "Nested runtime property types are a best-effort reflection fallback; strongly typed entry points annotate the root type.")]
    [UnconditionalSuppressMessage("Trimming", "IL2072", Justification = "Nested runtime property types are a best-effort reflection fallback; strongly typed entry points annotate the root type.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "The runtime-property fallback is best-effort for dynamic shapes; strongly typed entry points annotate the root type.")]
    private static Func<object, object?> BuildGetter([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] Type type, string path)
    {
        var segments = path.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
        // Pre-resolve property infos per segment (case-insensitive), but support dictionaries on the fly.
        var props = new PropertyInfo?[segments.Length];
        Type? current = type;
        for (int i = 0; i < segments.Length; i++)
        {
            string seg = segments[i];
            if (typeof(IDictionary).IsAssignableFrom(current))
            {
                // Mark with null; runtime will handle dictionary
                props[i] = null;
                // we cannot know next type statically; continue
                current = null;
                continue;
            }
            if (HasStringDictionaryInterface(current))
            {
                props[i] = null;
                current = null;
                continue;
            }

            var p = FindPropertyIgnoreCase(current, seg);
            props[i] = p;
            current = p?.PropertyType;
            if (current is null) break;
        }

        return (obj) =>
        {
            object? cur = obj;
            for (int i = 0; i < segments.Length; i++)
            {
                if (cur is null) return null;
                var p = props[i];
                if (p is null)
                {
                    if (TryGetFromListIndex(cur, segments[i], out var indexedValue))
                    {
                        cur = indexedValue;
                        continue;
                    }
                    // dictionary path
                    if (cur is IDictionary dict)
                    {
                        object? candidate = null;
                        if (TryGetFromDictionary(dict, segments[i], out candidate))
                        {
                            cur = candidate;
                            continue;
                        }
                        return MissingValue;
                    }
                    if (TryGetFromGenericDictionary(cur, segments[i], out var genVal))
                    {
                        cur = genVal;
                        continue;
                    }
                    // Not a dictionary; try reflective property fallback
                    var pr = FindPropertyIgnoreCase(cur.GetType(), segments[i]);
                    if (pr == null)
                    {
                        return MissingValue;
                    }
                    cur = pr.GetValue(cur);
                    continue;
                }
                cur = p.GetValue(cur);
            }
            return cur;
        };
    }

    private static bool TryGetFromListIndex(object value, string segment, out object? result)
    {
        result = null;
        if (!int.TryParse(segment, out var index) || index < 0)
        {
            return false;
        }

        if (value is IList list)
        {
            if (index >= list.Count)
            {
                result = MissingValue;
                return true;
            }

            result = list[index];
            return true;
        }

        return false;
    }

    private static bool TryGetFromDictionary(IDictionary dict, string key, out object? value)
    {
        value = null;
        if (dict.Contains(key)) { value = dict[key]; return true; }
        // case-insensitive search
        foreach (var k in dict.Keys)
        {
            if (k is string s && string.Equals(s, key, StringComparison.OrdinalIgnoreCase))
            {
                value = dict[k];
                return true;
            }
        }
        return false;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2072", Justification = "Generic dictionary probing is a best-effort fallback for runtime dictionary shapes.")]
    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Generic dictionary probing is a best-effort fallback for runtime dictionary shapes.")]
    private static bool TryGetFromGenericDictionary(object obj, string key, out object? value)
    {
        value = null;
        var dictionaryType = FindStringDictionaryInterface(obj.GetType());
        if (dictionaryType is null)
        {
            return false;
        }

        if (TryGetFromTypedStringDictionary(obj, dictionaryType, key, out value))
        {
            return true;
        }

        return TryGetFromStringDictionaryEnumeration(obj, dictionaryType, key, out value);
    }

    private static bool HasStringDictionaryInterface([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type? type)
        => FindStringDictionaryInterface(type) is not null;

    private static Type? FindStringDictionaryInterface([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type? type)
    {
        if (type is null) return null;

        if (type.IsGenericType)
        {
            var genericDefinition = type.GetGenericTypeDefinition();
            if ((genericDefinition == typeof(IDictionary<,>) || genericDefinition == typeof(IReadOnlyDictionary<,>))
                && type.GetGenericArguments()[0] == typeof(string))
            {
                return type;
            }
        }

        foreach (var i in type.GetInterfaces())
        {
            if (!i.IsGenericType)
            {
                continue;
            }

            var genericDefinition = i.GetGenericTypeDefinition();
            if ((genericDefinition == typeof(IDictionary<,>) || genericDefinition == typeof(IReadOnlyDictionary<,>))
                && i.GetGenericArguments()[0] == typeof(string))
            {
                return i;
            }
        }

        return null;
    }

    private static bool TryGetFromTypedStringDictionary(object obj, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties)] Type dictionaryType, string key, out object? value)
    {
        value = null;

        var containsKey = dictionaryType.GetMethod("ContainsKey");
        if (containsKey != null)
        {
            var found = containsKey.Invoke(obj, new object?[] { key });
            if (found is bool b && b)
            {
                var indexer = dictionaryType.GetProperty("Item");
                value = indexer?.GetValue(obj, new object?[] { key });
                return true;
            }
        }

        var tryGetValue = dictionaryType.GetMethod("TryGetValue");
        if (tryGetValue != null)
        {
            var parameters = new object?[] { key, null };
            var ok = (bool)tryGetValue.Invoke(obj, parameters)!;
            if (ok)
            {
                value = parameters[1];
                return true;
            }
        }

        return false;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2075", Justification = "Enumeration fallback reflects KeyValuePair-shaped entries after dictionary interface probing.")]
    private static bool TryGetFromStringDictionaryEnumeration(object obj, Type dictionaryType, string key, out object? value)
    {
        value = null;
        if (obj is not IEnumerable enumerable)
        {
            return false;
        }

        foreach (var entry in enumerable)
        {
            if (entry is null)
            {
                continue;
            }

            var entryType = entry.GetType();
            var keyProperty = entryType.GetProperty(nameof(KeyValuePair<string, object>.Key));
            var valueProperty = entryType.GetProperty(nameof(KeyValuePair<string, object>.Value));
            if (keyProperty is null || valueProperty is null)
            {
                continue;
            }

            var entryKey = keyProperty.GetValue(entry) as string;
            if (string.Equals(entryKey, key, StringComparison.OrdinalIgnoreCase))
            {
                value = valueProperty.GetValue(entry);
                return true;
            }
        }

        return false;
    }

    private static PropertyInfo? FindPropertyIgnoreCase([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type? type, string name)
    {
        if (type is null) return null;
        foreach (var property in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                return property;
            }
        }

        return null;
    }
}
