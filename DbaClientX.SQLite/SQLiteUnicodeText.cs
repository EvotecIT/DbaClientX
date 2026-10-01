using System;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Unicode case folding for SQLite connections, matching .NET's <see cref="string.ToLowerInvariant"/> (and the
/// browser's <c>toLowerCase()</c> except its special cases such as Turkish dotted I and final sigma), where SQLite's
/// own <c>lower()</c>, <c>LIKE</c> and <c>NOCASE</c> fold ASCII only.
/// </summary>
/// <remarks>
/// Register it on each connection, typically through <see cref="SQLite.ConfigureConnection"/>:
/// <c>new SQLite { ConfigureConnection = c =&gt; SQLiteUnicodeText.Register(c) }</c>. Then
/// <c>dbx_lower(Name) = dbx_lower(@p)</c> compares without case, <c>ORDER BY Name COLLATE DBX_NOCASE</c> sorts without
/// case, and <c>WhereContainsRaw("dbx_lower(Name)", text.ToLowerInvariant())</c> searches without case. The functions
/// run in .NET for every row they read, so they cost more than the built-ins on large scans.
/// <para>The mapping comes from the running .NET runtime's case tables (.NET Framework and .NET, and ICU versions,
/// differ for some letters). An index, generated column or <c>COLLATE DBX_NOCASE</c> column built with them stores that
/// runtime's order: every process that writes the database needs the same registration, and a <c>REINDEX</c> is
/// needed after moving to a runtime with different case tables. Built-in <c>lower()</c> and <c>upper()</c> are not
/// replaced: a pooled connection that had them replaced would leave the built-ins unusable for the next user.</para>
/// </remarks>
public static class SQLiteUnicodeText
{
    /// <summary>The scalar function that returns its text argument lower-cased with invariant rules.</summary>
    public const string LowerFunction = "dbx_lower";

    /// <summary>The scalar function that returns its text argument upper-cased with invariant rules.</summary>
    public const string UpperFunction = "dbx_upper";

    /// <summary>
    /// The collation that orders text by its invariant lower-case form, code unit by code unit (the order of
    /// <c>string.CompareOrdinal(a.ToLowerInvariant(), b.ToLowerInvariant())</c>).
    /// </summary>
    public const string NoCaseCollation = "DBX_NOCASE";

    /// <summary>
    /// Registers <see cref="LowerFunction"/>, <see cref="UpperFunction"/> and <see cref="NoCaseCollation"/> on an open
    /// connection.
    /// </summary>
    /// <param name="connection">An open connection.</param>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> is null.</exception>
    public static void Register(SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        connection.CreateFunction<string?, string?>(LowerFunction, static value => value?.ToLowerInvariant(), isDeterministic: true);
        connection.CreateFunction<string?, string?>(UpperFunction, static value => value?.ToUpperInvariant(), isDeterministic: true);
        connection.CreateCollation(NoCaseCollation, static (left, right) => Compare(left, right));
    }

    /// <summary>
    /// Compares two strings by their invariant lower-case forms, ordinally, without allocating for text in the Basic
    /// Multilingual Plane.
    /// </summary>
    /// <returns>A negative number, zero or a positive number, as <see cref="string.CompareOrdinal(string, string)"/>.</returns>
    public static int Compare(string? left, string? right)
    {
        if (ReferenceEquals(left, right))
        {
            return 0;
        }

        if (left == null)
        {
            return -1;
        }

        if (right == null)
        {
            return 1;
        }

        var length = Math.Min(left.Length, right.Length);
        for (var index = 0; index < length; index++)
        {
            var a = left[index];
            var b = right[index];
            if (char.IsSurrogate(a) || char.IsSurrogate(b))
            {
                // Letters outside the BMP fold as surrogate pairs; let the string mapping handle the rest.
                return string.CompareOrdinal(left.Substring(index).ToLowerInvariant(), right.Substring(index).ToLowerInvariant());
            }

            if (a != b)
            {
                var foldedA = char.ToLowerInvariant(a);
                var foldedB = char.ToLowerInvariant(b);
                if (foldedA != foldedB)
                {
                    return foldedA - foldedB;
                }
            }
        }

        return left.Length - right.Length;
    }
}
