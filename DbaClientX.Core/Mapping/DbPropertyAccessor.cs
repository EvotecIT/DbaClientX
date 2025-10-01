using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace DBAClientX.Mapping;

/// <summary>
/// Fast, case-insensitive property-path accessor for POCOs and dictionaries.
/// Supports dotted paths like "A.B.C" and falls back to dictionary lookups.
/// </summary>
public static class DbPropertyAccessor
{
    private static readonly ConcurrentDictionary<(Type, string), Func<object, object?>> _getterCache = new();

    /// <summary>
    /// Tries to resolve a value from an item using a dotted path. Case-insensitive.
    /// Handles POCOs (public properties) and dictionaries (IDictionary and IDictionary&lt;string,object&gt;).
    /// </summary>
    /// <param name="item">The source object (POCO or dictionary) to read from.</param>
    /// <param name="path">Dotted, case-insensitive path (e.g., "User.Name" or "Metadata.Tags.0").</param>
    /// <param name="value">When this method returns, contains the resolved value or null.</param>
    /// <returns>True when a value was resolved; otherwise false.</returns>
    public static bool TryGetValue(object? item, string path, out object? value)
    {
        value = null;
        if (item is null || string.IsNullOrWhiteSpace(path)) return false;

        var type = item.GetType();
        var key = (type, path);
        var getter = _getterCache.GetOrAdd(key, k => BuildGetter(k.Item1, k.Item2));
        if (getter is null) return false;
        value = getter(item);
        return true;
    }

    private static Func<object, object?> BuildGetter(Type type, string path)
    {
        var segments = path.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
        // Pre-resolve property infos per segment (case-insensitive), but support dictionaries on the fly
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
            if (ImplementsGenericDictionary(current))
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
                    // dictionary path
                    if (cur is IDictionary dict)
                    {
                        object? candidate = null;
                        if (TryGetFromDictionary(dict, segments[i], out candidate))
                        {
                            cur = candidate;
                            continue;
                        }
                        return null;
                    }
                    if (TryGetFromGenericDictionary(cur, segments[i], out var genVal))
                    {
                        cur = genVal;
                        continue;
                    }
                    // Not a dictionary; try reflective property fallback
                    var pr = FindPropertyIgnoreCase(cur.GetType(), segments[i]);
                    cur = pr?.GetValue(cur);
                    continue;
                }
                cur = p.GetValue(cur);
            }
            return cur;
        };
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

    private static bool TryGetFromGenericDictionary(object obj, string key, out object? value)
    {
        value = null;
        var t = obj.GetType();
        foreach (var i in t.GetInterfaces())
        {
            if (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>))
            {
                var args = i.GetGenericArguments();
                if (args[0] == typeof(string))
                {
                    var containsKey = i.GetMethod("ContainsKey");
                    var tryGetValue = i.GetMethod("TryGetValue");
                    if (containsKey != null)
                    {
                        var found = containsKey.Invoke(obj, new object?[] { key });
                        if (found is bool b && b)
                        {
                            var indexer = i.GetProperty("Item");
                            value = indexer?.GetValue(obj, new object?[] { key });
                            return true;
                        }
                    }
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
                }
            }
        }
        return false;
    }

    private static bool ImplementsGenericDictionary(Type? type)
    {
        if (type is null) return false;
        foreach (var i in type.GetInterfaces())
        {
            if (i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>))
            {
                return true;
            }
        }
        return false;
    }

    private static PropertyInfo? FindPropertyIgnoreCase(Type type, string name)
    {
        // Prefer case-insensitive name match, public instance properties
        var p = type.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
        return p;
    }
}
