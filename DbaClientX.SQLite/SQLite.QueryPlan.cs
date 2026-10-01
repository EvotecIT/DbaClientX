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
    /// <exception cref="ArgumentException">The text holds more than one statement or starts with <c>EXPLAIN</c>.</exception>
    /// <exception cref="NotSupportedException">The SQLite library is older than 3.24, whose plan rows this API does not read.</exception>
    /// <remarks>
    /// Needs SQLite 3.24 or later (the bundled library is newer). Connections opened for it run
    /// <see cref="ConfigureConnection"/>, so statements using registered functions or collations can be explained. A
    /// plan reflects the current data and statistics: run <c>ANALYZE</c> on a database shaped like production before
    /// asserting on it. SQLite prints full index scans and one-row aggregates (such as <c>MAX</c>, even a cheap
    /// <c>MAX</c> of an indexed column) as an unconstrained <c>SEARCH</c>; such steps are reported as scans.
    /// </remarks>
    public virtual async Task<DbaQueryPlan> ExplainQueryPlanAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default)
    {
        ValidateCommandText(query);
        if (!SqlStatementText.IsSingleStatement(query))
        {
            throw new ArgumentException("Explain one statement at a time; the text holds more than one.", nameof(query));
        }

        if (query.TrimStart().StartsWith("EXPLAIN", StringComparison.OrdinalIgnoreCase))
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
            "EXPLAIN QUERY PLAN " + query,
            reader => (reader.GetInt32(0), reader.GetInt32(1), reader.GetString(3)),
            parameters,
            cancellationToken).ConfigureAwait(false);
        return new DbaQueryPlan(query, SqliteQueryPlanParser.ParseAll(rows));
    }
}
