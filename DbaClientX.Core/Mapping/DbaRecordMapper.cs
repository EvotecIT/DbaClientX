using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Reflection;

namespace DBAClientX.Mapping;

/// <summary>
/// Creates mappers that project <see cref="IDataRecord"/> rows directly into typed objects, for use with the
/// <c>QueryStreamAsync&lt;T&gt;</c> and <c>QueryAsListAsync&lt;T&gt;</c> overloads.
/// </summary>
/// <remarks>
/// Streaming through a mapper keeps memory bounded: each row is read into a new object and nothing else is buffered,
/// unlike <see cref="DataTable"/> materialization or <see cref="DataRow"/> streaming.
/// </remarks>
public static class DbaRecordMapper
{
    /// <summary>
    /// Creates a mapper that sets public writable properties of <typeparamref name="T"/> from columns with the same name
    /// (case-insensitive).
    /// </summary>
    /// <typeparam name="T">Target type with a public parameterless constructor.</typeparam>
    /// <returns>A mapper for <c>QueryStreamAsync&lt;T&gt;</c>. It can be reused across queries and threads.</returns>
    /// <remarks>
    /// <list type="bullet">
    /// <item><description>
    /// Columns without a matching property are ignored; properties without a column keep their initial value. When two
    /// columns share a name, the first one is used.
    /// </description></item>
    /// <item><description><see cref="DBNull"/> sets <see langword="null"/>, or the default for non-nullable value types.</description></item>
    /// <item><description>
    /// Values are converted when needed: numeric conversions (<see cref="double"/> to <see cref="decimal"/> keeps about
    /// 15 significant digits), integers or names to enums, text to <see cref="Guid"/>, <see cref="DateTime"/>,
    /// <see cref="DateTimeOffset"/> and <see cref="TimeSpan"/> (invariant culture), and 16-byte arrays to
    /// <see cref="Guid"/> (.NET byte order, which differs from Oracle <c>RAW(16)</c> display order).
    /// </description></item>
    /// <item><description>
    /// Date and time values without an offset or kind are treated as UTC when converted to <see cref="DateTimeOffset"/>;
    /// <see cref="DateTimeOffset"/> values and text with an offset convert to UTC <see cref="DateTime"/>, and text without
    /// an offset keeps <see cref="DateTimeKind.Unspecified"/>.
    /// </description></item>
    /// </list>
    /// For hot paths, a hand-written <c>Func&lt;IDataRecord, T&gt;</c> that calls typed getters by ordinal is faster.
    /// </remarks>
    public static Func<IDataRecord, T> For<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>()
        where T : new()
        => new PropertyMapper<T>().Map;

    /// <summary>
    /// Creates a mapper that copies each row into a new value array, with <see cref="DBNull"/> replaced by <see langword="null"/>.
    /// </summary>
    /// <returns>A mapper for <c>QueryStreamAsync&lt;object?[]&gt;</c>, useful for building columnar chunks.</returns>
    public static Func<IDataRecord, object?[]> Values()
        => static record =>
        {
            var values = new object?[record.FieldCount];
            record.GetValues(values!);
            for (var index = 0; index < values.Length; index++)
            {
                if (values[index] is DBNull)
                {
                    values[index] = null;
                }
            }

            return values;
        };

    private sealed class PropertyMapper<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] T>
        where T : new()
    {
        private readonly Dictionary<string, PropertyInfo> _properties;
        private Binding? _binding;

        public PropertyMapper()
        {
            _properties = new Dictionary<string, PropertyInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var property in typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!property.CanWrite || property.SetMethod is not { IsPublic: true } || property.GetIndexParameters().Length != 0)
                {
                    continue;
                }

                // A property hidden with 'new' appears once per declaring type; keep the most derived one.
                if (!_properties.TryGetValue(property.Name, out var existing)
                    || property.DeclaringType!.IsSubclassOf(existing.DeclaringType!))
                {
                    _properties[property.Name] = property;
                }
            }
        }

        public T Map(IDataRecord record)
        {
            // Providers can reuse one reader object for several queries (Npgsql does per connection), so the binding is
            // validated against the column names rather than the reader identity.
            var binding = _binding;
            if (binding == null || !binding.Matches(record))
            {
                binding = Bind(record);
                _binding = binding;
            }

            var item = new T();
            for (var index = 0; index < binding.Ordinals.Length; index++)
            {
                var ordinal = binding.Ordinals[index];
                var property = binding.Properties[index];
                var value = record.IsDBNull(ordinal) ? null : Convert(record.GetValue(ordinal), property, binding.Columns[ordinal]);
                try
                {
                    property.SetValue(item, value);
                }
                catch (TargetInvocationException exception)
                {
                    throw new InvalidOperationException(
                        $"Setting property '{property.Name}' from column '{binding.Columns[ordinal]}' failed.",
                        exception.InnerException ?? exception);
                }
            }

            return item;
        }

        private Binding Bind(IDataRecord record)
        {
            var columns = new string[record.FieldCount];
            var ordinals = new List<int>();
            var properties = new List<PropertyInfo>();
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (var ordinal = 0; ordinal < columns.Length; ordinal++)
            {
                var name = record.GetName(ordinal);
                columns[ordinal] = name;
                if (seen.Add(name) && _properties.TryGetValue(name, out var property))
                {
                    ordinals.Add(ordinal);
                    properties.Add(property);
                }
            }

            return new Binding(columns, ordinals.ToArray(), properties.ToArray());
        }

        private static object Convert(object value, PropertyInfo property, string column)
        {
            var target = Nullable.GetUnderlyingType(property.PropertyType) ?? property.PropertyType;
            if (target.IsInstanceOfType(value))
            {
                return value;
            }

            try
            {
                return value switch
                {
                    string text when target.IsEnum => Enum.Parse(target, text, ignoreCase: true),
                    _ when target.IsEnum => Enum.ToObject(target, System.Convert.ChangeType(value, Enum.GetUnderlyingType(target), CultureInfo.InvariantCulture)!),
                    string text when target == typeof(Guid) => Guid.Parse(text),
                    byte[] { Length: 16 } bytes when target == typeof(Guid) => new Guid(bytes),
                    string text when target == typeof(DateTime) => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal),
                    string text when target == typeof(DateTimeOffset) => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                    DateTime dateTime when target == typeof(DateTimeOffset) => new DateTimeOffset(
                        dateTime.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dateTime, DateTimeKind.Utc) : dateTime),
                    DateTimeOffset dateTimeOffset when target == typeof(DateTime) => dateTimeOffset.UtcDateTime,
                    string text when target == typeof(TimeSpan) => TimeSpan.Parse(text, CultureInfo.InvariantCulture),
                    _ => System.Convert.ChangeType(value, target, CultureInfo.InvariantCulture)!,
                };
            }
            catch (Exception exception) when (exception is InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                throw new InvalidCastException(
                    $"Cannot convert column '{column}' value of type '{value.GetType().Name}' to property '{property.Name}' of type '{property.PropertyType.Name}'.",
                    exception);
            }
        }

        private sealed class Binding
        {
            public Binding(string[] columns, int[] ordinals, PropertyInfo[] properties)
            {
                Columns = columns;
                Ordinals = ordinals;
                Properties = properties;
            }

            public string[] Columns { get; }

            public int[] Ordinals { get; }

            public PropertyInfo[] Properties { get; }

            public bool Matches(IDataRecord record)
            {
                if (record.FieldCount != Columns.Length)
                {
                    return false;
                }

                for (var ordinal = 0; ordinal < Columns.Length; ordinal++)
                {
                    if (!string.Equals(record.GetName(ordinal), Columns[ordinal], StringComparison.Ordinal))
                    {
                        return false;
                    }
                }

                return true;
            }
        }
    }
}
