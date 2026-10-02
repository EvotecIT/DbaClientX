namespace DBAClientX.QueryBuilder;

/// <summary>How a text condition such as <see cref="Query.WhereContains(string, string, TextFolding)"/> treats letter case.</summary>
public enum TextFolding
{
    /// <summary>
    /// Compare as the database compares text: <c>LIKE</c>, which follows the column's collation on SQL Server and
    /// MySQL (their default collations ignore case), and a case-sensitive <c>instr()</c> on SQLite.
    /// </summary>
    None,

    /// <summary>
    /// Fold both sides with the database's own function: <c>LOWER</c>, or <c>ILIKE</c> on PostgreSQL. SQLite's
    /// <c>lower()</c> folds ASCII letters only, so <c>Ż</c> does not match <c>ż</c> there; use <see cref="Invariant"/>.
    /// </summary>
    Database,

    /// <summary>
    /// Fold both sides with .NET's invariant lower-casing (<see cref="string.ToLowerInvariant"/>), for every letter.
    /// SQLite only: the column is folded by <c>dbx_lower()</c>, which <c>SQLiteUnicodeText.Register</c> must have added
    /// to the connection (the statement fails with "no such function" otherwise), and the text is folded in .NET.
    /// Other dialects throw <see cref="System.NotSupportedException"/> when compiled; their <c>LOWER</c> folds Unicode
    /// letters under a Unicode-aware collation or locale (not PostgreSQL's <c>C</c> locale), so use <see cref="Database"/>
    /// there.
    /// </summary>
    /// <remarks>
    /// <c>dbx_lower()</c> runs in .NET for every row the condition reads. Counting the rows of 1,000,000 that contain a
    /// text took 290 to 350 ms for ASCII values (160 to 210 ms with <see cref="Database"/>) and 490 to 590 ms for
    /// values with letters beyond ASCII (220 to 310 ms with <see cref="Database"/>, which misses their other case). For
    /// large tables, store a folded copy of the column and search it with <see cref="None"/> and folded text, or narrow
    /// the rows with a trigram index first.
    /// </remarks>
    Invariant
}
