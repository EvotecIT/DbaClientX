using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.QueryPlans;

namespace DBAClientX;

public partial class SQLite
{
    private static readonly Version MinimumQueryPlanVersion = new(3, 24, 0);

    /// <summary>
    /// Returns the plan SQLite chooses for a statement (<c>EXPLAIN QUERY PLAN</c>), as structured steps, without
    /// running the statement.
    /// </summary>
    /// <param name="database">Path to the SQLite database file; it is opened read-only.</param>
    /// <param name="query">One statement, without the <c>EXPLAIN</c> prefix.</param>
    /// <param name="parameters">Values for the statement's parameters. Every named parameter must have a value (any
    /// value of the right type): SQLite plans the statement as it will run.</param>
    /// <param name="cancellationToken">Stops the work.</param>
    /// <returns>The plan, for <see cref="QueryPlanAssert"/> or inspection.</returns>
    /// <exception cref="ArgumentException">The text holds no statement or more than one (<see cref="SqlStatementText.Split(string)"/>),
    /// or starts with <c>EXPLAIN</c>.</exception>
    /// <exception cref="NotSupportedException">The SQLite library is older than 3.24, whose plan rows this API does not read.</exception>
    /// <remarks>
    /// Needs SQLite 3.24 or later (the bundled library is newer). Connections opened for it run
    /// <see cref="ConfigureConnection"/>, so statements using registered functions or collations can be explained. A
    /// plan reflects the current data and statistics: run <c>ANALYZE</c> on a database shaped like production before
    /// asserting on it, or seed its statistics (<see cref="WritePlannerStatisticsAsync"/>).
    /// <para>Steps carry what the plan text leaves out. <see cref="DbaQueryPlanStep.EstimatedRows"/> and
    /// <see cref="DbaQueryPlanStep.TableRows"/> come from <c>sqlite_stat1</c>: a scan reads the table's rows, and a search
    /// the average rows per key of the index columns its equality constraints cover (unknown for a range alone).
    /// SQLite prints a <c>MIN</c>/<c>MAX</c> that reads one row at the end of an index and one that walks it alike (as a
    /// <c>SEARCH</c>, unconstrained when nothing narrows it, which is otherwise a full index scan). Such a step is a
    /// <see cref="DbaQueryPlanOperation.Search"/> of one row when the statement looks up one column of the table
    /// (<c>SELECT MAX(Seen) FROM t [WHERE …]</c>, also inside <c>COALESCE</c>/<c>IFNULL</c> or a subquery), the index
    /// (primary key or rowid) serves every condition of its <c>WHERE</c> (an <c>IN</c> of several values does not
    /// count), the column is the index column after the equalities or the one a range bounds, and the index orders it
    /// under the column's own collation; otherwise an unconstrained one is a scan.</para>
    /// </remarks>
    public virtual async Task<DbaQueryPlan> ExplainQueryPlanAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        ValidateCommandText(query);
        var statements = SqlStatementText.Split(query);
        if (statements.Count != 1)
        {
            throw new ArgumentException(
                statements.Count == 0 ? "The text holds no statement to explain." : "Explain one statement at a time; the text holds more than one.",
                nameof(query));
        }

        // Explain the statement without the semicolons and comments around it.
        var statement = statements[0];
        if (statement.StartsWith("EXPLAIN", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("Pass the statement without EXPLAIN.", nameof(query));
        }

        var version = await QueryReadOnlyAsListAsync(database, "SELECT sqlite_version()", reader => reader.GetString(0), cancellationToken: cancellationToken).ConfigureAwait(false);
        if (!Version.TryParse(version[0], out var parsed) || parsed < MinimumQueryPlanVersion)
        {
            throw new NotSupportedException(string.Format(CultureInfo.InvariantCulture, "Query plans need SQLite {0} or later; the library is {1}.", MinimumQueryPlanVersion, version[0]));
        }

        var rows = await QueryReadOnlyAsListAsync(
            database,
            "EXPLAIN QUERY PLAN " + statement,
            reader => (reader.GetInt32(0), reader.GetInt32(1), reader.GetString(3)),
            parameters,
            cancellationToken).ConfigureAwait(false);
        var plan = new DbaQueryPlan(query, SqliteQueryPlanParser.ParseAll(rows));
        return await SqliteQueryPlanEstimates.ApplyAsync(this, database, plan, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// The collation a table column of the main database is declared with (<c>BINARY</c> by default), or null when the
    /// column does not exist or the SQLite library cannot tell (built without column metadata).
    /// </summary>
    internal string? GetColumnCollation(string database, string table, string column)
    {
        try
        {
            using var connection = new Microsoft.Data.Sqlite.SqliteConnection(BuildOperationalConnectionString(database, readOnly: true));
            connection.Open();
            int resultCode = SQLitePCL.raw.sqlite3_table_column_metadata(
                connection.Handle,
                "main",
                table,
                column,
                out string _,
                out string collation,
                out int _,
                out int _,
                out int _);
            return resultCode == SQLitePCL.raw.SQLITE_OK ? collation : null;
        }
        catch (Exception exception) when (exception is EntryPointNotFoundException or Microsoft.Data.Sqlite.SqliteException)
        {
            return null;
        }
    }
}
