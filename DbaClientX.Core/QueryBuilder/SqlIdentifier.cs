using System;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Quotes identifiers (table, column, alias and schema names) for a SQL dialect, the way the query compiler does.
/// </summary>
/// <remarks>
/// Use it to build raw SQL fragments (for <c>SelectRaw</c>, <c>WhereRaw</c> and similar) from names that come from a
/// mapping or a user: a quoted identifier is always read as one name, whatever characters it holds.
/// </remarks>
public static class SqlIdentifier
{
    /// <summary>Quotes one identifier part for <paramref name="dialect"/>.</summary>
    /// <param name="dialect">The target dialect: <c>[name]</c> for SQL Server, <c>`name`</c> for MySQL and
    /// <c>"name"</c> for PostgreSQL, SQLite and Oracle.</param>
    /// <param name="identifier">The name as stored. A dot is part of the name; it never separates a schema.</param>
    /// <returns>The quoted identifier, with the closing quote character doubled inside it.</returns>
    /// <exception cref="ArgumentException">
    /// The identifier is null or empty, contains a NUL character, or contains a double quote for Oracle, which
    /// cannot express one in a quoted identifier.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="dialect"/> is not a defined dialect.</exception>
    /// <remarks>
    /// SQLite reads a double-quoted name that matches no column as a string literal (its legacy behavior for double
    /// quotes); quoting does not change which names exist. Keep the result in Unicode text: converting it to a narrow
    /// code page with best-fit mapping can turn look-alike characters into quote characters.
    /// </remarks>
    public static string Quote(SqlDialect dialect, string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            throw new ArgumentException("Identifier cannot be null or empty.", nameof(identifier));
        }

        if (identifier.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("Identifier cannot contain a NUL character.", nameof(identifier));
        }

        var (open, close) = Delimiters(dialect);
        if (dialect == SqlDialect.Oracle && identifier.IndexOf('"') >= 0)
        {
            throw new ArgumentException("Oracle identifiers cannot contain a double quote.", nameof(identifier));
        }

        return open + identifier.Replace(close.ToString(), new string(close, 2)) + close;
    }

    private static (char Open, char Close) Delimiters(SqlDialect dialect) => dialect switch
    {
        SqlDialect.SqlServer => ('[', ']'),
        SqlDialect.MySql => ('`', '`'),
        SqlDialect.PostgreSql => ('"', '"'),
        SqlDialect.SQLite => ('"', '"'),
        SqlDialect.Oracle => ('"', '"'),
        _ => throw new ArgumentOutOfRangeException(nameof(dialect), dialect, "Unsupported SQL dialect.")
    };
}
