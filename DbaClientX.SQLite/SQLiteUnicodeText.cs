using System;
using Microsoft.Data.Sqlite;
using SQLitePCL;

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
/// run in .NET for every row they read, so they cost more than the built-ins on large scans. The collation compares the
/// stored UTF-8 bytes in place and allocates nothing per comparison (on .NET Framework and .NET Standard builds, text
/// with characters outside the Basic Multilingual Plane still allocates): a sort of 1,000,000 short texts took about
/// twice the time of a <c>BINARY</c> sort.
/// <para>The mapping comes from the running .NET runtime's case tables (.NET Framework and .NET, and ICU versions,
/// differ for some letters). An index, generated column or <c>COLLATE DBX_NOCASE</c> column built with them stores that
/// runtime's order: every process that writes the database needs the same registration, and a <c>REINDEX</c> is
/// needed after moving to a runtime with different case tables. Built-in <c>lower()</c> and <c>upper()</c> are not
/// replaced: a pooled connection that had them replaced would leave the built-ins unusable for the next user.</para>
/// </remarks>
public static partial class SQLiteUnicodeText
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

    private static readonly delegate_collation NoCaseUtf8 = static (_, left, right) => CompareUtf8(left, right);

    /// <summary>
    /// Registers <see cref="LowerFunction"/>, <see cref="UpperFunction"/> and <see cref="NoCaseCollation"/> on an open
    /// connection.
    /// </summary>
    /// <param name="connection">An open connection with no statement running (no open reader).</param>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The connection is not open, or a statement on it is running.</exception>
    /// <remarks>
    /// The registrations last until the connection closes, also when the provider pools its handle. If the same
    /// <see cref="SqliteConnection"/> is opened again without calling this method, the provider registers the
    /// collation again with its string-based comparison, which orders the same way but allocates two strings per
    /// comparison. The collation name <c>dbx_registration_probe</c> is reserved for the check that no statement runs;
    /// SQL and schemas must not use it.
    /// </remarks>
    public static void Register(SqliteConnection connection)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (connection.State != System.Data.ConnectionState.Open)
        {
            throw new InvalidOperationException("Register Unicode text functions on an open connection.");
        }

        var handle = connection.Handle!;
        ThrowIfStatementRuns(handle);

        connection.CreateFunction<string?, string?>(LowerFunction, static value => value?.ToLowerInvariant(), isDeterministic: true);
        connection.CreateFunction<string?, string?>(UpperFunction, static value => value?.ToUpperInvariant(), isDeterministic: true);

        // The provider marshals both collation arguments to strings, two allocations per comparison (gigabytes for one
        // sort of a million rows). Registering through it first makes it remove the collation by name when the
        // connection closes or returns to the pool; the native registration that follows replaces the callback with one
        // that reads the UTF-8 bytes in place.
        connection.CreateCollation(NoCaseCollation, static (left, right) => Compare(left, right));
        var result = raw.sqlite3__create_collation_utf8(handle, NoCaseCollation, null, NoCaseUtf8);
        SqliteException.ThrowExceptionForRC(result, handle);
    }

    /// <summary>A collation name reserved for <see cref="ThrowIfStatementRuns"/>; SQL and schemas must not name it.</summary>
    private const string RegistrationProbe = "dbx_registration_probe";

    private static readonly delegate_collation ProbeComparison = static (_, _, _) => 0;

    /// <summary>
    /// Throws when a statement runs on the connection. SQLite refuses to replace a function or collation then, and
    /// SQLitePCLRaw releases the previous callback before it asks, so a refused replacement would leave SQLite holding a
    /// released callback. SQLitePCLRaw does not list statements by default, so the check replaces a private probe
    /// collation: a refusal can leave only the probe, which nothing compares with, holding a released callback.
    /// </summary>
    private static void ThrowIfStatementRuns(sqlite3 handle)
    {
        // The first registration creates the probe (or replaces one left by a refusal); the second always replaces it.
        if (raw.sqlite3__create_collation_utf8(handle, RegistrationProbe, null, ProbeComparison) == raw.SQLITE_BUSY ||
            raw.sqlite3__create_collation_utf8(handle, RegistrationProbe, null, ProbeComparison) == raw.SQLITE_BUSY)
        {
            throw new InvalidOperationException("Register Unicode text functions before running statements on the connection; a statement (an open reader) is running.");
        }

        var result = raw.sqlite3__create_collation_utf8(handle, RegistrationProbe, null, null);
        SqliteException.ThrowExceptionForRC(result, handle);
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

        return CompareFolded(left.AsSpan(), right.AsSpan());
    }
}
