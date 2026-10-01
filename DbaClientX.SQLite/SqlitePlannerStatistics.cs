using System.Globalization;

namespace DBAClientX;

/// <summary>
/// One row of SQLite's planner statistics (<c>sqlite_stat1</c>): how many rows a table has and, for an index, how many
/// rows share each prefix of its key on average.
/// </summary>
/// <remarks>
/// <c>ANALYZE</c> writes these rows and the planner reads them when a connection loads the schema. Writing chosen
/// values (<see cref="SQLite.WritePlannerStatisticsAsync"/>) makes a small test database plan like a large one, and
/// copying them from a production-sized database (<see cref="SQLite.ReadPlannerStatisticsAsync"/>) makes it plan like
/// that one. The bundled SQLite library is built without <c>SQLITE_ENABLE_STAT4</c>, so it neither writes nor reads
/// <c>sqlite_stat4</c>; these rows are all the statistics it uses.
/// </remarks>
public sealed class SqlitePlannerStatistics
{
    /// <summary>Creates a statistics row.</summary>
    /// <param name="table">The table.</param>
    /// <param name="index">The index, the table's own name for the primary key of a <c>WITHOUT ROWID</c> table, or
    /// <see langword="null"/> for a row that gives the table's row count only.</param>
    /// <param name="rowCount">The rows in the table (for an index, the entries in the index, which is the same unless the
    /// index is partial).</param>
    /// <param name="rowsPerKey">For an index, the average rows sharing each key prefix: the first value for the first
    /// column, the second for the first two columns, and so on (<c>1</c> for a unique prefix), one per key column. SQLite
    /// keeps its defaults for the columns a shorter list leaves out, unless options follow: then it reads them as
    /// unique, so <see cref="SQLite.WritePlannerStatisticsAsync"/> refuses such a row. Empty for a table row.</param>
    /// <param name="options">Further words SQLite reads after the numbers (<c>unordered</c>, <c>noskipscan</c>, <c>sz=N</c>).</param>
    /// <exception cref="ArgumentException">A name is empty, a table row has key counts, a count is out of range, or an
    /// option is empty, holds a space or starts with a digit.</exception>
    public SqlitePlannerStatistics(string table, string? index, long rowCount, IReadOnlyList<long>? rowsPerKey = null, IReadOnlyList<string>? options = null)
    {
        if (string.IsNullOrWhiteSpace(table))
        {
            throw new ArgumentException("The table name is required.", nameof(table));
        }

        if (index != null && index.Trim().Length == 0)
        {
            throw new ArgumentException("The index name cannot be empty; use null for a table row.", nameof(index));
        }

        if (rowCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(rowCount), "The row count cannot be negative.");
        }

        long[] perKey = rowsPerKey?.ToArray() ?? Array.Empty<long>();
        if (index == null && perKey.Length > 0)
        {
            throw new ArgumentException("A table row (no index) has no key prefix counts.", nameof(rowsPerKey));
        }

        if (perKey.Any(value => value < 1))
        {
            throw new ArgumentOutOfRangeException(nameof(rowsPerKey), "Rows per key prefix must be at least 1.");
        }

        string[] words = options?.ToArray() ?? Array.Empty<string>();
        if (words.Any(word => string.IsNullOrWhiteSpace(word) || word.Any(char.IsWhiteSpace) || IsAsciiDigit(word[0])))
        {
            throw new ArgumentException("Options must be single words that do not start with a digit.", nameof(options));
        }

        Table = table;
        Index = index;
        RowCount = rowCount;
        RowsPerKey = perKey;
        Options = words;
    }

    /// <summary>Gets the table.</summary>
    public string Table { get; }

    /// <summary>Gets the index, the table's name for a <c>WITHOUT ROWID</c> primary key, or <see langword="null"/> for a table row.</summary>
    public string? Index { get; }

    /// <summary>Gets the rows in the table (or entries in the index).</summary>
    public long RowCount { get; }

    /// <summary>Gets the average rows sharing each key prefix of the index, shortest prefix first.</summary>
    public IReadOnlyList<long> RowsPerKey { get; }

    /// <summary>Gets the words after the numbers (<c>unordered</c>, <c>noskipscan</c>, <c>sz=N</c>).</summary>
    public IReadOnlyList<string> Options { get; }

    /// <summary>Returns the <c>stat</c> column text SQLite stores for this row (<c>"1000000 500 1"</c>).</summary>
    /// <returns>The text.</returns>
    public string ToStatText()
        => string.Join(" ", new[] { RowCount }.Concat(RowsPerKey).Select(value => value.ToString(CultureInfo.InvariantCulture)).Concat(Options));

    /// <summary>Reads a row as SQLite stores it.</summary>
    /// <param name="table">The <c>tbl</c> column.</param>
    /// <param name="index">The <c>idx</c> column.</param>
    /// <param name="stat">The <c>stat</c> column.</param>
    /// <returns>The row.</returns>
    /// <exception cref="FormatException">The text does not start with a row count.</exception>
    /// <exception cref="ArgumentException">The table name is empty, or the index name is empty but not null.</exception>
    public static SqlitePlannerStatistics Parse(string table, string? index, string stat)
    {
        // Read as SQLite does: the leading digits of each word while words start with one ("1000.0" is 1000), then the
        // option words; numbers among the options mean nothing to it and are dropped.
        string[] words = (stat ?? string.Empty).Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
        var numbers = new List<long>();
        int position = 0;
        while (position < words.Length && IsAsciiDigit(words[position][0]))
        {
            numbers.Add(LeadingNumber(words[position]));
            position++;
        }

        if (numbers.Count == 0)
        {
            throw new FormatException($"The statistics of '{table}' do not start with a row count: '{stat}'.");
        }

        // SQLite stores 0 for an empty index's prefix counts; the planner treats it as 1.
        return new SqlitePlannerStatistics(
            table,
            index,
            numbers[0],
            index == null ? null : numbers.Skip(1).Select(value => Math.Max(1, value)).ToArray(),
            words.Skip(position).Where(word => !IsAsciiDigit(word[0])).ToArray());
    }

    private static bool IsAsciiDigit(char character) => character is >= '0' and <= '9';

    private static long LeadingNumber(string word)
    {
        long value = 0;
        foreach (char character in word)
        {
            if (character < '0' || character > '9')
            {
                break;
            }

            value = value > (long.MaxValue - (character - '0')) / 10 ? long.MaxValue : value * 10 + (character - '0');
        }

        return value;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Table} {Index ?? "(table)"}: {ToStatText()}";
}
