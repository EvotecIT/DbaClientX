using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
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
    public static bool TryGetValue(object? item, string path, out object? value)
    {
        value = null;
        if (item is null || string.IsNullOrWhiteSpace(path)) return false;

        var type = item.GetType();
        var key = (type, path);
        var getter = _getterCache.GetOrAdd(key, k => BuildGetter(k.Item1, k.Item2));
        if (getter is null) return false;
        value = getter(item);
        if (ReferenceEquals(value, MissingValue))
        {
            value = null;
            return false;
        }
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

        // If all segments resolved to properties (no dictionaries), build a compiled expression fast path
        bool allProps = true;
        for (int i = 0; i < props.Length; i++) { if (props[i] == null) { allProps = false; break; } }
        if (allProps && props.Length > 0)
        {
            try
            {
                var objParam = Expression.Parameter(typeof(object), "obj");
                // root may be null or not of the expected type
                var rootAs = Expression.TypeAs(objParam, type);

                Expression BuildChain(Expression curExpr, int index)
                {
                    if (index >= props.Length)
                    {
                        return Expression.Convert(curExpr, typeof(object));
                    }
                    var prop = props[index]!;
                    var access = Expression.Property(curExpr, prop);
                    var t = prop.PropertyType;
                    bool isRef = !t.IsValueType;
                    bool isNullableValue = Nullable.GetUnderlyingType(t) != null;
                    if (isRef || isNullableValue)
                    {
                        return Expression.Condition(
                            Expression.Equal(access, Expression.Constant(null, t)),
                            Expression.Constant(null, typeof(object)),
                            BuildChain(access, index + 1));
                    }
                    return BuildChain(access, index + 1);
                }

                var body = Expression.Condition(
                    Expression.Equal(rootAs, Expression.Constant(null, type)),
                    Expression.Constant(null, typeof(object)),
                    BuildChain(rootAs, 0));
                var lambda = Expression.Lambda<Func<object, object?>>(body, objParam);
                return lambda.Compile();
            }
            catch
            {
                // fall back to reflective path below
            }
        }

        // Fallback: reflective/dictionary-aware path
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

    private static bool HasStringDictionaryInterface(Type? type)
        => FindStringDictionaryInterface(type) is not null;

    private static Type? FindStringDictionaryInterface(Type? type)
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

    private static bool TryGetFromTypedStringDictionary(object obj, Type dictionaryType, string key, out object? value)
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

    private static bool TryGetFromStringDictionaryEnumeration(object obj, Type dictionaryType, string key, out object? value)
    {
        value = null;
        if (obj is not IEnumerable enumerable)
        {
            return false;
        }

        var valueType = dictionaryType.GetGenericArguments()[1];
        var expectedPairType = typeof(KeyValuePair<,>).MakeGenericType(typeof(string), valueType);
        var keyProperty = expectedPairType.GetProperty(nameof(KeyValuePair<string, object>.Key));
        var valueProperty = expectedPairType.GetProperty(nameof(KeyValuePair<string, object>.Value));
        if (keyProperty is null || valueProperty is null)
        {
            return false;
        }

        foreach (var entry in enumerable)
        {
            if (entry is null || !expectedPairType.IsInstanceOfType(entry))
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

    private static PropertyInfo? FindPropertyIgnoreCase(Type? type, string name)
    {
        if (type is null) return null;
        // Prefer case-insensitive name match, public instance properties
        var p = type.GetProperty(name, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
        return p;
    }
}
