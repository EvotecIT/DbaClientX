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
    public KeysetColumn(string column, bool descending = false, string? resultColumn = null)
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
    }

    /// <summary>Gets the column identifier used in <c>WHERE</c> and <c>ORDER BY</c>.</summary>
    public string Column { get; }

    /// <summary>Gets a value indicating whether the key is sorted in descending order.</summary>
    public bool Descending { get; }

    /// <summary>Gets the name of the result column that holds the key value.</summary>
    public string ResultColumn { get; }

    /// <summary>Creates an ascending key column.</summary>
    /// <param name="column">Column identifier.</param>
    /// <returns>The key column.</returns>
    public static KeysetColumn Asc(string column) => new(column);

    /// <summary>Creates a descending key column.</summary>
    /// <param name="column">Column identifier.</param>
    /// <returns>The key column.</returns>
    public static KeysetColumn Desc(string column) => new(column, descending: true);
}
