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
    /// Gets or sets whether sorting or grouping in a temporary B-tree, in a query step that reads a large table, is a
    /// violation. Defaults to <see langword="true"/>.
    /// </summary>
    public bool ForbidTempBTrees { get; set; } = true;

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
