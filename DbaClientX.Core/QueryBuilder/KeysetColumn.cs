using System;
using System.Collections.Generic;

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
        : this(column, descending, resultColumn, valueType, isExpression: false, collation: null)
    {
    }

    private KeysetColumn(string column, bool descending, string? resultColumn, Type? valueType, bool isExpression, string? collation)
    {
        if (string.IsNullOrWhiteSpace(column))
        {
            throw new ArgumentException(isExpression ? "Expression cannot be null or whitespace." : "Column cannot be null or whitespace.", isExpression ? "expression" : nameof(column));
        }

        if (resultColumn != null && string.IsNullOrWhiteSpace(resultColumn))
        {
            throw new ArgumentException("Result column cannot be whitespace.", nameof(resultColumn));
        }

        Column = column;
        Descending = descending;
        IsExpression = isExpression;
        Collation = collation;
        ResultColumn = resultColumn ?? column.Substring(column.LastIndexOf('.') + 1);
        ValueType = valueType == null ? null : Nullable.GetUnderlyingType(valueType) ?? valueType;
        if (ValueType is { IsEnum: true })
        {
            throw new ArgumentException("Enum key types are not supported; declare the underlying integer type.", nameof(valueType));
        }
    }

    /// <summary>
    /// Gets the column identifier used in <c>WHERE</c> and <c>ORDER BY</c>, or the SQL expression when
    /// <see cref="IsExpression"/> is set.
    /// </summary>
    public string Column { get; }

    /// <summary>
    /// Gets a value indicating whether <see cref="Column"/> is a trusted SQL expression written as is, created by
    /// <see cref="Expression"/>.
    /// </summary>
    public bool IsExpression { get; }

    /// <summary>
    /// Gets the collation the key is compared and sorted with (<c>column COLLATE name</c> in both <c>WHERE</c> and
    /// <c>ORDER BY</c>), or <see langword="null"/> for the column's own.
    /// </summary>
    public string? Collation { get; }

    /// <summary>
    /// Gets a value indicating whether two different rows can have equal keys although their stored values differ: an
    /// expression or a collation. Such a key cannot be the last, unique key of a <see cref="KeysetPagination"/>.
    /// </summary>
    internal bool CanTieDistinctValues => IsExpression || Collation != null;

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

    /// <summary>
    /// Creates a key on a trusted SQL expression, for an order no plain column gives: for example SQLite's
    /// <c>+"CreatedUtc"</c>, which orders by the column without letting its index decide the plan, or PostgreSQL's
    /// <c>"Name" COLLATE "und-x-icu"</c>, a collation name <see cref="WithCollation"/> does not accept.
    /// </summary>
    /// <param name="expression">Trusted SQL written as is in <c>WHERE</c> and <c>ORDER BY</c>, without added
    /// parentheses: wrap an expression that contains operators. Never pass untrusted input.</param>
    /// <param name="resultColumn">The column of the query results that holds the key value; select the expression under
    /// this name.</param>
    /// <param name="descending">Whether the key is sorted in descending order.</param>
    /// <param name="valueType">Optional CLR type of the key values; see the constructor.</param>
    /// <returns>The key column.</returns>
    /// <exception cref="ArgumentException">The expression or result column is empty, or <paramref name="valueType"/> is an enum.</exception>
    /// <remarks>
    /// An expression can give different rows equal keys, so it cannot be the last key: end the key with a unique plain
    /// column (see <see cref="KeysetPagination(int, IEnumerable{KeysetColumn}, KeysetColumn)"/>). An expression has no
    /// column affinity on SQLite (<c>+"Seen"</c> included), so cursor values are compared as bound, without conversion:
    /// declare <paramref name="valueType"/> as the stored type the reader returns (for example <see cref="long"/>).
    /// </remarks>
    public static KeysetColumn Expression(string expression, string resultColumn, bool descending = false, Type? valueType = null)
    {
        if (string.IsNullOrWhiteSpace(resultColumn))
        {
            throw new ArgumentException("An expression key needs the result column that holds its value.", nameof(resultColumn));
        }

        return new KeysetColumn(expression, descending, resultColumn, valueType, isExpression: true, collation: null);
    }

    /// <summary>
    /// Returns a copy of this column key compared and sorted with <paramref name="collation"/>
    /// (<c>column COLLATE name</c>).
    /// </summary>
    /// <param name="collation">A collation name of letters, digits and underscores that does not start with a digit, such
    /// as <c>NOCASE</c>, <c>DBX_NOCASE</c> (with <c>SQLiteUnicodeText</c> registered), <c>Latin1_General_100_CI_AS</c>
    /// or <c>C</c>. PostgreSQL quotes it, so its case counts there; other dialects write it as is. For other names use
    /// <see cref="Expression"/>.</param>
    /// <returns>The key column with the collation.</returns>
    /// <exception cref="ArgumentException">The name is not a plain collation name.</exception>
    /// <exception cref="InvalidOperationException">The key is an expression; write <c>COLLATE</c> inside it.</exception>
    /// <remarks>
    /// A collation can make different values equal (<c>a</c> and <c>A</c>), so a collated key cannot be the last key:
    /// end the key with a unique plain column. An index serves the order only when it uses the same collation. Oracle
    /// accepts <c>COLLATE</c> in expressions only from 12.2 with extended data types, and with <c>DISTINCT</c> SQL Server
    /// and PostgreSQL require the collated expression in the select list.
    /// </remarks>
    public KeysetColumn WithCollation(string collation)
    {
        if (IsExpression)
        {
            throw new InvalidOperationException("An expression key carries its own COLLATE; write it inside the expression.");
        }

        if (!IsPlainName(collation))
        {
            throw new ArgumentException("A collation name holds only letters, digits and underscores and does not start with a digit; use KeysetColumn.Expression for other names.", nameof(collation));
        }

        return new KeysetColumn(Column, Descending, ResultColumn, ValueType, isExpression: false, collation);
    }

    private static bool IsPlainName(string? name)
    {
        if (string.IsNullOrEmpty(name) || char.IsDigit(name![0]))
        {
            return false;
        }

        foreach (var character in name)
        {
            if (!(character is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '_'))
            {
                return false;
            }
        }

        return true;
    }
}
