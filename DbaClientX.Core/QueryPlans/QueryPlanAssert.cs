using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryPlans;

/// <summary>
/// Checks query plans against <see cref="QueryPlanRules"/>, for tests and startup guards in any test framework: a
/// failure throws <see cref="QueryPlanViolationException"/>, and <see cref="Check"/> returns the violations instead.
/// </summary>
/// <remarks>
/// Plans depend on the data, the statistics (<c>ANALYZE</c>) and the database version, so check them against a
/// database whose large tables hold enough rows and statistics to make the planner choose as in production (for
/// SQLite, <c>SQLite.WritePlannerStatisticsAsync</c> seeds them). A plan shows which index a step uses, not how many
/// rows it reads: a search counts as wide only when the statistics say so (<see cref="DbaQueryPlanStep.EstimatedRows"/>)
/// or when it searches a range open on one side, so without statistics a search on a non-selective key passes.
/// </remarks>
public static class QueryPlanAssert
{
    /// <summary>Returns the steps of <paramref name="plan"/> that break <paramref name="rules"/>.</summary>
    /// <param name="plan">The plan to check.</param>
    /// <param name="rules">The rules.</param>
    /// <returns>The result, with every violation.</returns>
    public static QueryPlanCheckResult Check(DbaQueryPlan plan, QueryPlanRules rules)
    {
        if (plan == null)
        {
            throw new ArgumentNullException(nameof(plan));
        }

        if (rules == null)
        {
            throw new ArgumentNullException(nameof(rules));
        }

        if (plan.Provenance.Dialect != DBAClientX.QueryBuilder.SqlDialect.SQLite)
            throw new NotSupportedException("QueryPlanRules use SQLite identifier, scan and statistics semantics. Native provider plans require provider-specific assessment.");

        var violations = new List<QueryPlanViolation>();
        if (rules.RequireLargeTablesInPlan)
        {
            foreach (var table in rules.LargeTables.Where(table => !plan.Steps.Any(step => plan.TablesOf(step).Contains(table, SqliteIdentifierComparer.Instance))))
            {
                violations.Add(new QueryPlanViolation(QueryPlanViolationKind.TableNotInPlan, table, null));
            }
        }

        foreach (var step in plan.Steps)
        {
            switch (step.Operation)
            {
                case DbaQueryPlanOperation.Search when rules.ForbidWideSearches && IsWideSearch(plan, step, rules):
                    foreach (var table in plan.TablesOf(step).Where(rules.AppliesTo))
                    {
                        violations.Add(new QueryPlanViolation(QueryPlanViolationKind.WideSearch, table, step));
                    }

                    break;
                case DbaQueryPlanOperation.Scan when rules.ForbidFullScans:
                    foreach (var table in plan.TablesOf(step).Where(rules.AppliesTo))
                    {
                        violations.Add(new QueryPlanViolation(QueryPlanViolationKind.FullScan, table, step));
                    }

                    break;
                case DbaQueryPlanOperation.AutomaticIndex when rules.ForbidAutomaticIndexes:
                    foreach (var table in plan.TablesOf(step).Where(rules.AppliesTo))
                    {
                        violations.Add(new QueryPlanViolation(QueryPlanViolationKind.AutomaticIndex, table, step));
                    }

                    break;
                case DbaQueryPlanOperation.TempBTree when rules.ForbidTempBTrees:
                    // A temporary B-tree sorts the rows of its query step: blame the large tables that step (or a
                    // co-routine or subquery under it) reads in full or through a wide search. Rows a selective search
                    // narrows are cheap to sort.
                    foreach (var table in TablesReadWidelyUnder(plan, step.ParentId, rules).OfType<string>().Where(rules.AppliesTo).Distinct(SqliteIdentifierComparer.Instance))
                    {
                        violations.Add(new QueryPlanViolation(QueryPlanViolationKind.TempBTree, table!, step));
                    }

                    break;
            }
        }

        return new QueryPlanCheckResult(plan, violations);
    }

    /// <summary>Throws when <paramref name="plan"/> reads every row of one of <paramref name="largeTables"/>.</summary>
    /// <param name="plan">The plan to check.</param>
    /// <param name="largeTables">Tables too large to scan; each must appear in the plan. None means every table.</param>
    /// <exception cref="QueryPlanViolationException">The plan scans one of the tables, or does not name it.</exception>
    public static void NoFullScan(DbaQueryPlan plan, params string[] largeTables)
        => Check(plan, new QueryPlanRules(largeTables) { ForbidTempBTrees = false, ForbidAutomaticIndexes = false, ForbidWideSearches = false }).ThrowIfViolated();

    /// <summary>
    /// Throws when <paramref name="plan"/> scans, searches widely (<see cref="QueryPlanRules.ForbidWideSearches"/>), sorts
    /// in a temporary B-tree, or builds an automatic index over one of <paramref name="largeTables"/>.
    /// </summary>
    /// <param name="plan">The plan to check.</param>
    /// <param name="largeTables">Tables too large for those operations; each must appear in the plan. None means every table.</param>
    /// <exception cref="QueryPlanViolationException">The plan breaks a rule.</exception>
    public static void UsesIndexes(DbaQueryPlan plan, params string[] largeTables)
        => Check(plan, new QueryPlanRules(largeTables)).ThrowIfViolated();

    /// <summary>
    /// Whether a search reads much of its table, more than the rules' share of its rows by the statistics or a range
    /// open on one side, and no <c>LIMIT</c> stops it first.
    /// </summary>
    private static bool IsWideSearch(DbaQueryPlan plan, DbaQueryPlanStep step, QueryPlanRules rules)
    {
        var wide = (step.EstimatedRows is { } rows && step.TableRows is > 0 && rows > rules.WideSearchFraction * step.TableRows.Value) ||
                   step.IsOpenEndedRange;
        return wide && !IsStoppedByLimit(plan, step, rules);
    }

    /// <summary>
    /// Whether a <c>LIMIT</c> stops reading the step's rows: the step is the outer loop of its query (the first step under
    /// its parent), nothing under that parent sorts or groups the rows first, and the query that reads the table stops
    /// early of its own (<see cref="SqlQueryLevels.HasLimitWhereRead"/>: a <c>LIMIT</c> without an aggregate or
    /// <c>GROUP BY</c>, or an <c>EXISTS</c> subquery).
    /// </summary>
    private static bool IsStoppedByLimit(DbaQueryPlan plan, DbaQueryPlanStep step, QueryPlanRules rules)
    {
        var siblings = plan.Steps.Where(other => other.ParentId == step.ParentId).ToArray();
        if (!ReferenceEquals(siblings[0], step))
        {
            return false;
        }

        // A full sort or grouping reads every row first. A sort of the last terms only (RIGHT PART OF ORDER BY, LAST TERM
        // OF ORDER BY) sorts the rows of each value of the leading ones as they come: cheap, and stopped by the limit, when
        // the index reads a range of that leading column (few rows per value), not one value of it.
        var sorts = siblings.Where(other => other.Operation == DbaQueryPlanOperation.TempBTree && other.TempBTreePurpose != null).ToArray();
        if (sorts.Any(other => other.TempBTreePurpose is "ORDER BY" or "GROUP BY"))
        {
            return false;
        }

        var partialSort = sorts.Any(other => other.TempBTreePurpose!.EndsWith("ORDER BY", StringComparison.Ordinal));
        return SqlQueryLevels.HasLimitWhereRead(
            plan.Sql,
            step.Table!,
            nested: step.ParentId != 0,
            step.IndexConstraintColumns,
            partialSort ? column => SortsFewRowsPerValue(step, column, rules) : null,
            step.TableRows is > 0 ? step.TableRows.Value * rules.WideSearchFraction : null);
    }

    /// <summary>
    /// Whether a partial sort by the terms after <paramref name="column"/> sorts few rows at a time: the statistics give
    /// the index's rows per value of its key up to that column, at most the rules' share of the table (unknown without
    /// statistics, then assumed small, as for a search).
    /// </summary>
    private static bool SortsFewRowsPerValue(DbaQueryPlanStep step, string column, QueryPlanRules rules)
    {
        if (step.IndexRowsPerKey == null || step.TableRows is not > 0)
        {
            return true;
        }

        foreach (var (key, rows) in step.IndexRowsPerKey)
        {
            if (SqliteIdentifierComparer.Instance.Equals(key, column))
            {
                return rows <= rules.WideSearchFraction * step.TableRows.Value;
            }
        }

        return false;
    }

    private static IEnumerable<string?> TablesReadWidelyUnder(DbaQueryPlan plan, int parentId, QueryPlanRules rules)
    {
        var children = plan.Steps.ToLookup(step => step.ParentId);
        var pending = new Stack<int>();
        pending.Push(parentId);
        var visited = new HashSet<int>();
        while (pending.Count > 0)
        {
            var id = pending.Pop();
            if (!visited.Add(id))
            {
                continue;
            }

            foreach (var step in children[id])
            {
                if (step.Operation is DbaQueryPlanOperation.Scan or DbaQueryPlanOperation.AutomaticIndex ||
                    (step.Operation == DbaQueryPlanOperation.Search && IsWideSearch(plan, step, rules)))
                {
                    foreach (var table in plan.TablesOf(step))
                    {
                        yield return table;
                    }
                }

                if (step.Id != id)
                {
                    pending.Push(step.Id);
                }
            }
        }
    }
}
