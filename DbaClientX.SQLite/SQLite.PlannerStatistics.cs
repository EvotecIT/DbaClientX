using System.Globalization;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>Returns the planner statistics of a database (the rows of <c>sqlite_stat1</c>).</summary>
    /// <param name="database">Path to the SQLite database file; it is opened read-only.</param>
    /// <param name="cancellationToken">Stops the work.</param>
    /// <returns>The statistics the planner uses, one row per table and index (where several rows name the same ones
    /// without regard to case, the last one written, as SQLite takes it); empty when the database was never analyzed.
    /// Rows without a table, statistics text or a leading row count are left out.</returns>
    /// <remarks>
    /// Read them from a production-sized database and write them to a small one with
    /// <see cref="WritePlannerStatisticsAsync"/>, so plans checked on the small one are the plans the large one gets.
    /// </remarks>
    public virtual async Task<IReadOnlyList<SqlitePlannerStatistics>> ReadPlannerStatisticsAsync(string database, CancellationToken cancellationToken = default)
    {
        var exists = await QueryReadOnlyAsListAsync(
            database,
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_stat1'",
            reader => reader.GetInt64(0),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        if (exists[0] == 0)
        {
            return Array.Empty<SqlitePlannerStatistics>();
        }

        // SQLite loads the rows in rowid order and a later row for the same names (compared without case) wins.
        var rows = await QueryReadOnlyAsListAsync(
            database,
            "SELECT CAST(tbl AS TEXT), CAST(idx AS TEXT), CAST(stat AS TEXT) FROM sqlite_stat1 WHERE tbl IS NOT NULL AND stat IS NOT NULL ORDER BY rowid",
            reader => (Table: reader.GetString(0), Index: reader.IsDBNull(1) ? null : reader.GetString(1), Stat: reader.GetString(2)),
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var statistics = new Dictionary<(string, string), SqlitePlannerStatistics>();
        foreach (var row in rows)
        {
            try
            {
                statistics[(DataMovement.DbaIdentifierPath.NormalizeSqliteIdentifier(row.Table),
                    row.Index == null ? string.Empty : DataMovement.DbaIdentifierPath.NormalizeSqliteIdentifier(row.Index))] = SqlitePlannerStatistics.Parse(row.Table, row.Index, row.Stat);
            }
            catch (Exception exception) when (exception is FormatException or ArgumentException)
            {
                // A row without a leading row count (or with an empty name) gives the planner nothing to use.
            }
        }

        return statistics.Values
            .OrderBy(row => row.Table, StringComparer.OrdinalIgnoreCase)
            .ThenBy(row => row.Index, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    /// <summary>
    /// Writes planner statistics to a database (the rows of <c>sqlite_stat1</c>), so its queries plan as if its tables
    /// held the rows the statistics describe; for example a small test database checked by <see cref="ExplainQueryPlanAsync"/>
    /// and <see cref="QueryPlans.QueryPlanAssert"/>.
    /// </summary>
    /// <param name="database">Path to the SQLite database file.</param>
    /// <param name="statistics">The rows to write; a row replaces the existing row of the same table and index (names
    /// compare without case, as SQLite matches them).</param>
    /// <param name="replaceAll">Whether every existing row is removed first, so only the given rows remain. With no rows
    /// this removes every statistic: SQLite then assumes about a million rows per table and ten rows per index key, as
    /// for a database never analyzed.</param>
    /// <param name="cancellationToken">Stops the work; nothing is written.</param>
    /// <returns>A task that completes when the statistics are written.</returns>
    /// <exception cref="ArgumentException">A row is null, or a row with options gives an existing index a key prefix count
    /// for other than each of its key columns (SQLite would read the missing ones as unique).</exception>
    /// <remarks>
    /// <para>The rows are written in one transaction, creating <c>sqlite_stat1</c> when missing
    /// (<c>ANALYZE sqlite_schema</c>, which analyzes no user table). The transaction also creates and drops an empty
    /// table so the schema version changes: every open connection, pooled ones included, reloads the schema and the new
    /// statistics when its next statement reads the database outside a transaction it already had open (an
    /// <c>EXPLAIN QUERY PLAN</c> alone does not read it, so it can still plan with the old ones;
    /// <see cref="ExplainQueryPlanAsync"/> opens a new connection). Rows for tables or indexes that do not exist are
    /// written and ignored by SQLite. A table's row count is taken from its table row and from each index row that is
    /// not partial, the one loaded last winning, so keep them equal.</para>
    /// <para>Statistics describe rows, not values: the planner learns how many rows a key matches on average, not which
    /// values are rare. A later <c>ANALYZE</c> (or <c>PRAGMA optimize</c>, which may run one) writes measured rows that
    /// win over written ones; it removes only rows spelled as the schema spells the names.</para>
    /// </remarks>
    public virtual async Task WritePlannerStatisticsAsync(
        string database,
        IEnumerable<SqlitePlannerStatistics> statistics,
        bool replaceAll = false,
        CancellationToken cancellationToken = default)
    {
        if (statistics == null)
        {
            throw new ArgumentNullException(nameof(statistics));
        }

        SqlitePlannerStatistics[] rows = statistics.ToArray();
        if (rows.Any(row => row == null))
        {
            throw new ArgumentException("A statistics row is null.", nameof(statistics));
        }

        await ValidateKeyCountsAsync(database, rows, cancellationToken).ConfigureAwait(false);
        var script = new StringBuilder();
        var parameters = new Dictionary<string, object?>();
        script.Append("ANALYZE sqlite_schema; ");
        if (replaceAll)
        {
            script.Append("DELETE FROM sqlite_stat1; ");
        }

        for (int index = 0; index < rows.Length; index++)
        {
            string number = index.ToString(CultureInfo.InvariantCulture);
            script.Append("DELETE FROM sqlite_stat1 WHERE tbl = @t").Append(number).Append(" COLLATE NOCASE AND idx IS @i").Append(number).Append(" COLLATE NOCASE; ");
            script.Append("INSERT INTO sqlite_stat1 (tbl, idx, stat) VALUES (@t").Append(number).Append(", @i").Append(number).Append(", @s").Append(number).Append("); ");
            parameters["@t" + number] = rows[index].Table;
            parameters["@i" + number] = rows[index].Index;
            parameters["@s" + number] = rows[index].ToStatText();
        }

        // A schema change makes every connection reload the schema, and with it the statistics.
        string marker = "dbx_reload_statistics_" + Guid.NewGuid().ToString("N");
        script.Append("CREATE TABLE ").Append(marker).Append(" (x); DROP TABLE ").Append(marker).Append(';');
        string sql = script.ToString();
        await using SQLiteAsyncSession session = await OpenSessionAsync(database, cancellationToken).ConfigureAwait(false);
        await session.RunInTransactionAsync(
            (transaction, token) => transaction.ExecuteNonQueryAsync(sql, parameters, token),
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Refuses an index row with options whose key prefix counts do not match the index's key columns: SQLite reads the
    /// option words in place of missing counts as unique prefixes.
    /// </summary>
    private async Task ValidateKeyCountsAsync(string database, IReadOnlyList<SqlitePlannerStatistics> rows, CancellationToken cancellationToken)
    {
        if (!File.Exists(database))
        {
            // The write creates the database: it has no indexes to check against yet.
            return;
        }

        foreach (SqlitePlannerStatistics row in rows.Where(row => row.Index != null && row.Options.Count > 0))
        {
            var keyColumns = await QueryReadOnlyAsListAsync(
                database,
                "SELECT COUNT(*) FROM pragma_index_xinfo(@index) WHERE key = 1",
                reader => reader.GetInt64(0),
                new Dictionary<string, object?> { ["@index"] = row.Index },
                cancellationToken).ConfigureAwait(false);
            if (keyColumns[0] > 0 && keyColumns[0] != row.RowsPerKey.Count)
            {
                throw new ArgumentException(
                    $"The statistics of index '{row.Index}' give {row.RowsPerKey.Count} key prefix counts for its {keyColumns[0]} key columns; with options, give one per column.",
                    "statistics");
            }
        }
    }
}
