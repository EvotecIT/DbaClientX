using System;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Describes one column of a keyset (seek) paging key.
/// </summary>
public sealed class KeysetColumn
{
    /// <summary>
    /// Initializes a new key column.
    /// </summary>
    /// <param name="column">
    /// Unquoted column identifier used in <c>WHERE</c> and <c>ORDER BY</c>, for example <c>Id</c> or <c>t.Id</c>. The
    /// compiler quotes each segment. It cannot reference a <c>SELECT</c> alias or an aggregate.
    /// </param>
    /// <param name="descending">Whether the key is sorted in descending order.</param>
    /// <param name="resultColumn">
    /// Name of the column in query results that holds the key value. Defaults to the last segment of <paramref name="column"/>.
    /// </param>
    /// <param name="valueType">
    /// Optional CLR type of the key values, exactly as the provider returns them (for example SQLite returns dates and GUIDs
    /// as <see cref="string"/>, Oracle <c>NUMBER(19)</c> as <see cref="decimal"/>). When set, cursor values of another type
    /// are rejected; integer types convert to each other within range. Enums are not supported; declare the underlying
    /// integer type.
    /// </param>
    /// <exception cref="ArgumentException"><paramref name="valueType"/> is an enum.</exception>
    public KeysetColumn(string column, bool descending = false, string? resultColumn = null, Type? valueType = null)
    {
        if (string.IsNullOrWhiteSpace(column))
        {
            throw new ArgumentException("Column cannot be null or whitespace.", nameof(column));
        }

        if (resultColumn != null && string.IsNullOrWhiteSpace(resultColumn))
        {
            throw new ArgumentException("Result column cannot be whitespace.", nameof(resultColumn));
        }

        Column = column;
        Descending = descending;
        ResultColumn = resultColumn ?? column.Substring(column.LastIndexOf('.') + 1);
        ValueType = valueType == null ? null : Nullable.GetUnderlyingType(valueType) ?? valueType;
        if (ValueType is { IsEnum: true })
        {
            throw new ArgumentException("Enum key types are not supported; declare the underlying integer type.", nameof(valueType));
        }
    }

    /// <summary>Gets the column identifier used in <c>WHERE</c> and <c>ORDER BY</c>.</summary>
    public string Column { get; }

    /// <summary>Gets a value indicating whether the key is sorted in descending order.</summary>
    public bool Descending { get; }

    /// <summary>Gets the name of the result column that holds the key value.</summary>
    public string ResultColumn { get; }

    /// <summary>
    /// Gets the expected CLR type of key values, or <see langword="null"/> when cursor values are not type-checked. Values
    /// must match it exactly, except that integer types convert to each other within range.
    /// </summary>
    public Type? ValueType { get; }

    /// <summary>Creates an ascending key column.</summary>
    /// <param name="column">Column identifier.</param>
    /// <returns>The key column.</returns>
    public static KeysetColumn Asc(string column) => new(column);

    /// <summary>Creates a descending key column.</summary>
    /// <param name="column">Column identifier.</param>
    /// <returns>The key column.</returns>
    public static KeysetColumn Desc(string column) => new(column, descending: true);

    /// <summary>Creates an ascending key column whose cursor values must be <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">CLR type of the key values.</typeparam>
    /// <param name="column">Column identifier.</param>
    /// <returns>The key column.</returns>
    public static KeysetColumn Asc<T>(string column) => new(column, valueType: typeof(T));

    /// <summary>Creates a descending key column whose cursor values must be <typeparamref name="T"/>.</summary>
    /// <typeparam name="T">CLR type of the key values.</typeparam>
    /// <param name="column">Column identifier.</param>
    /// <returns>The key column.</returns>
    public static KeysetColumn Desc<T>(string column) => new(column, descending: true, valueType: typeof(T));
}
