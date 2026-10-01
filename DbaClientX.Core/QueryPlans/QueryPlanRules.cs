using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

/// <summary>What <see cref="QueryPlanAssert.Check"/> reports as a violation.</summary>
public sealed class QueryPlanRules
{
    /// <summary>Creates rules for the named large tables, with every check on.</summary>
    /// <param name="largeTables">Tables too large to read in full; names compare without case. Empty means every table.</param>
    public QueryPlanRules(params string[] largeTables)
    {
        LargeTables = new HashSet<string>(largeTables ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>Gets the tables too large to read in full. When empty, the rules apply to every table.</summary>
    public ISet<string> LargeTables { get; }

    /// <summary>Gets or sets whether reading every row of a large table is a violation. Defaults to <see langword="true"/>.</summary>
    public bool ForbidFullScans { get; set; } = true;

    /// <summary>
    /// Gets or sets whether sorting or grouping in a temporary B-tree, in a query step that reads a large table in full or
    /// through a wide search (as <see cref="ForbidWideSearches"/> defines it, whether or not that rule is on), is a
    /// violation: a top-N <c>ORDER BY … LIMIT</c> sorts every row the search finds. Defaults to <see langword="true"/>.
    /// </summary>
    public bool ForbidTempBTrees { get; set; } = true;

    /// <summary>
    /// Gets or sets whether a search that reads much of a large table is a violation. Defaults to <see langword="true"/>.
    /// A search is wide when the statistics say it reads more than <see cref="WideSearchFraction"/> of the table's rows
    /// (<see cref="DbaQueryPlanStep.EstimatedRows"/>), or when it searches a range bounded on one side only
    /// (<see cref="DbaQueryPlanStep.IsOpenEndedRange"/>), which can read every row; unless a <c>LIMIT</c> stops it: it is
    /// the first step of its query, nothing sorts or groups all its rows first (a sort of the last <c>ORDER BY</c> terms
    /// only while the index reads a range of the first, whose values hold few rows by the statistics), the query reads
    /// that one table, each term of its <c>WHERE</c> is
    /// a bare comparison of columns the index search constrains or a keyset seek on the <c>ORDER BY</c> columns that
    /// names the first while the index reads a range of it, and the query has a <c>LIMIT</c> of its own without an
    /// aggregate or <c>GROUP BY</c> (or is an <c>EXISTS</c> subquery). A <c>MIN</c>/<c>MAX</c> that reads one
    /// end of an index is a one-row search. <c>col IS NOT NULL</c> prints as a range open on one side, and is reported
    /// even when few rows hold a value.
    /// </summary>
    public bool ForbidWideSearches { get; set; } = true;

    /// <summary>
    /// Gets or sets the share of a table's rows above which a search counts as wide (see <see cref="ForbidWideSearches"/>).
    /// Defaults to 0.1: an index that leaves more than a tenth of the rows costs about as much as reading them all.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is not between 0 and 1.</exception>
    public double WideSearchFraction
    {
        get => _wideSearchFraction;
        set => _wideSearchFraction = value is > 0 and <= 1
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), "The fraction must be above 0 and at most 1.");
    }

    private double _wideSearchFraction = 0.1;

    /// <summary>
    /// Gets or sets whether an automatic (statement-only) index on a large table is a violation: the database builds
    /// it from every row because no stored index fits. Defaults to <see langword="true"/>.
    /// </summary>
    public bool ForbidAutomaticIndexes { get; set; } = true;

    /// <summary>
    /// Gets or sets whether every named large table must appear in the plan under its name. Defaults to
    /// <see langword="true"/>, so a typo, a schema prefix or an alias the rules cannot resolve fails instead of passing;
    /// set it to <see langword="false"/> when the same rules check statements that do not read every named table.
    /// </summary>
    public bool RequireLargeTablesInPlan { get; set; } = true;

    /// <summary>Returns whether the rules cover <paramref name="table"/>.</summary>
    /// <param name="table">A table name from a plan step.</param>
    /// <returns><see langword="true"/> when the table is large, or when no large tables are named.</returns>
    public bool AppliesTo(string? table)
        => table != null && (LargeTables.Count == 0 || LargeTables.Contains(table));
}
