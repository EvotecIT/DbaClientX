using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DBAClientX.QueryBuilder;

namespace DBAClientX;

/// <summary>
/// Substring search through an FTS5 trigram index, the indexed form of "contains" for large SQLite tables.
/// </summary>
/// <remarks>
/// <para>Create the index with <see cref="SQLite.CreateTrigramIndexAsync"/>: an FTS5 table holding a copy of chosen
/// text columns, kept current by triggers. A query then finds the keys of matching rows through
/// <see cref="MatchingKeys"/> instead of scanning every row:
/// <c>query.WhereInRaw(SqlIdentifier.Quote(SqlDialect.SQLite, "Id"), SQLiteTrigramSearch.MatchingKeys("HostsSearch", text))</c>.</para>
/// <para>Matching ignores case with SQLite's Unicode case folding (close to, but not the same as, .NET's
/// <c>ToLowerInvariant</c>; accents are kept) and treats every character, including <c>%</c>, <c>_</c> and quotes,
/// literally. The text must hold at least three characters (<see cref="CanMatch"/>); shorter text cannot use the index,
/// so fall back to <c>WhereContains</c>. The index finds stored column text; formatted values (dates, numbers as
/// displayed) must be stored in a column to be found.</para>
/// <para>Measured on 1,000,000 rows: counting the rows that contain a rare text takes 5 ms instead of 189 ms for a
/// scan, <c>_lab</c> (1% of rows) 42 ms instead of 187 ms, and a text found in a fifth of the rows 144 ms instead of
/// 287 ms. The first page of a common text is faster with a scan that stops after one page (5 ms against 69 ms), and
/// "does not contain" is slower through the index (1,024 ms against 206 ms for <c>WhereNot</c> over a scan), so use
/// the index for counts and selective searches.</para>
/// </remarks>
public static class SQLiteTrigramSearch
{
    /// <summary>The fewest characters a text needs for the trigram index to find it.</summary>
    public const int MinimumLength = 3;

    /// <summary>Returns whether <paramref name="text"/> is long enough for the trigram index (three characters or more).</summary>
    /// <param name="text">The text to find.</param>
    /// <returns><see langword="true"/> when the text holds at least <see cref="MinimumLength"/> Unicode characters.</returns>
    public static bool CanMatch(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var characters = 0;
        for (var index = 0; index < text!.Length; index++)
        {
            if (char.IsHighSurrogate(text[index]) && index + 1 < text.Length && char.IsLowSurrogate(text[index + 1]))
            {
                index++;
            }

            if (++characters >= MinimumLength)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Returns <paramref name="text"/> as an FTS5 phrase: in double quotes with inner quotes doubled, so FTS5 operators,
    /// column filters and wildcards in it are matched as text.
    /// </summary>
    /// <param name="text">The text to quote.</param>
    /// <returns>The phrase, for use as a <c>MATCH</c> parameter value.</returns>
    /// <exception cref="ArgumentException">The text is null or contains a NUL character.</exception>
    public static string Phrase(string text)
    {
        if (text == null)
        {
            throw new ArgumentException("Text cannot be null.", nameof(text));
        }

        if (text.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("Text cannot contain a NUL character.", nameof(text));
        }

        return "\"" + text.Replace("\"", "\"\"") + "\"";
    }

    /// <summary>
    /// Creates a query of the keys (rowids) of the rows whose indexed text contains <paramref name="text"/>, for
    /// <c>WhereInRaw</c>, <c>WhereNotInRaw</c> or a join.
    /// </summary>
    /// <param name="indexName">The trigram index created by <see cref="SQLite.CreateTrigramIndexAsync"/>.</param>
    /// <param name="text">The text to find, at least three characters (see <see cref="CanMatch"/>).</param>
    /// <param name="column">One indexed column to search, or <see langword="null"/> for all of them.</param>
    /// <returns>A query selecting <c>rowid</c>; the text is a parameter.</returns>
    /// <exception cref="ArgumentException">The text is shorter than three characters or contains a NUL character, or a
    /// name is empty.</exception>
    public static Query MatchingKeys(string indexName, string text, string? column = null)
    {
        var index = SqlIdentifier.Quote(SqlDialect.SQLite, indexName);
        if (!CanMatch(text))
        {
            throw new ArgumentException($"The trigram index needs at least {MinimumLength} characters; use WhereContains for shorter text.", nameof(text));
        }

        if (column != null && column.Length == 0)
        {
            throw new ArgumentException("Column cannot be empty.", nameof(column));
        }

        var match = column == null ? Phrase(text) : Phrase(column) + " : " + Phrase(text);
        return new Query().SelectRaw("rowid").FromRaw(index).WhereRaw(index, "MATCH", match);
    }

    /// <summary>Builds the statements that create a trigram index, its triggers, and fill it from its table.</summary>
    /// <remarks>
    /// The FTS5 table keeps its own copy of the indexed text, keyed by the table's rowid, and every trigger removes
    /// entries by rowid, so writes that skip delete triggers (<c>INSERT OR REPLACE</c> without recursive triggers)
    /// cannot corrupt it: an entry left for a row deleted that way points at no row and is replaced when its rowid is
    /// reused. The update trigger fires on any update and acts when the rowid or an indexed value (generated columns
    /// included) changed, whichever name the update used.
    /// </remarks>
    internal static string CreateScript(string indexName, string table, IReadOnlyList<string> columns)
        => "BEGIN IMMEDIATE;\n" + CreateStatements(indexName, table, columns) + "COMMIT;";

    /// <summary>Builds the statements that drop and recreate a trigram index, so it matches its table again.</summary>
    internal static string RebuildScript(string indexName, string table, IReadOnlyList<string> columns)
        => "BEGIN IMMEDIATE;\n" + DropStatements(indexName) + CreateStatements(indexName, table, columns) + "COMMIT;";

    /// <summary>Builds the statements that remove a trigram index and its triggers.</summary>
    internal static string DropScript(string indexName)
        => "BEGIN IMMEDIATE;\n" + DropStatements(indexName) + "COMMIT;";

    private static string DropStatements(string indexName)
        => $"DROP TRIGGER IF EXISTS main.{Trigger(indexName, "ai")};\n" +
           $"DROP TRIGGER IF EXISTS main.{Trigger(indexName, "ad")};\n" +
           $"DROP TRIGGER IF EXISTS main.{Trigger(indexName, "au")};\n" +
           $"DROP TABLE IF EXISTS main.{SqlIdentifier.Quote(SqlDialect.SQLite, indexName)};\n";

    private static string CreateStatements(string indexName, string table, IReadOnlyList<string> columns)
    {
        if (columns == null || columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required.", nameof(columns));
        }

        var index = SqlIdentifier.Quote(SqlDialect.SQLite, indexName);
        var quotedTable = SqlIdentifier.Quote(SqlDialect.SQLite, table);
        var quoted = columns.Select(column => SqlIdentifier.Quote(SqlDialect.SQLite, column)).ToArray();
        if (quoted.Distinct(StringComparer.OrdinalIgnoreCase).Count() != quoted.Length)
        {
            throw new ArgumentException("Columns must be distinct.", nameof(columns));
        }

        var list = string.Join(", ", quoted);
        var newValues = string.Join(", ", quoted.Select(column => "new." + column));
        var changed = string.Join(" OR ", new[] { "old.rowid IS NOT new.rowid" }.Concat(quoted.Select(column => $"old.{column} IS NOT new.{column}")));
        // Statements inside a trigger cannot name a schema; the trigger lives in main with its table.
        var deleteOld = $"DELETE FROM {index} WHERE rowid = old.rowid;";
        var deleteNew = $"DELETE FROM {index} WHERE rowid = new.rowid;";
        var insertNew = $"INSERT INTO {index}(rowid, {list}) VALUES (new.rowid, {newValues});";

        var script = new StringBuilder();
        script.Append($"CREATE VIRTUAL TABLE main.{index} USING fts5({list}, tokenize='trigram');\n");
        script.Append($"CREATE TRIGGER main.{Trigger(indexName, "ai")} AFTER INSERT ON {quotedTable} BEGIN {deleteNew} {insertNew} END;\n");
        script.Append($"CREATE TRIGGER main.{Trigger(indexName, "ad")} AFTER DELETE ON {quotedTable} BEGIN {deleteOld} END;\n");
        script.Append($"CREATE TRIGGER main.{Trigger(indexName, "au")} AFTER UPDATE ON {quotedTable} WHEN {changed} BEGIN {deleteOld} {deleteNew} {insertNew} END;\n");
        script.Append($"INSERT INTO main.{index}(rowid, {list}) SELECT rowid, {list} FROM main.{quotedTable};\n");
        return script.ToString();
    }
    private static string Trigger(string indexName, string suffix)
        => SqlIdentifier.Quote(SqlDialect.SQLite, indexName + "_" + suffix);
}
