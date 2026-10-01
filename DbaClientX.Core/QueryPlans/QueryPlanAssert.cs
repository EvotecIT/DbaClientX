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
/// database whose large tables hold enough rows and statistics to make the planner choose as in production. A plan
/// shows which index a step uses, not how many rows a range reads: a search over a wide range passes.
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

        var violations = new List<QueryPlanViolation>();
        if (rules.RequireLargeTablesInPlan)
        {
            foreach (var table in rules.LargeTables.Where(table => !plan.Steps.Any(step => plan.TablesOf(step).Contains(table, StringComparer.OrdinalIgnoreCase))))
            {
                violations.Add(new QueryPlanViolation(QueryPlanViolationKind.TableNotInPlan, table, null));
            }
        }

        foreach (var step in plan.Steps)
        {
            switch (step.Operation)
            {
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
                    // co-routine or subquery under it) reads in full. Rows a search narrows are cheap to sort.
                    foreach (var table in TablesReadInFullUnder(plan, step.ParentId).Where(rules.AppliesTo).Distinct(StringComparer.OrdinalIgnoreCase))
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
        => Check(plan, new QueryPlanRules(largeTables) { ForbidTempBTrees = false, ForbidAutomaticIndexes = false }).ThrowIfViolated();

    /// <summary>
    /// Throws when <paramref name="plan"/> scans, sorts in a temporary B-tree, or builds an automatic index over one of
    /// <paramref name="largeTables"/>.
    /// </summary>
    /// <param name="plan">The plan to check.</param>
    /// <param name="largeTables">Tables too large for those operations; each must appear in the plan. None means every table.</param>
    /// <exception cref="QueryPlanViolationException">The plan breaks a rule.</exception>
    public static void UsesIndexes(DbaQueryPlan plan, params string[] largeTables)
        => Check(plan, new QueryPlanRules(largeTables)).ThrowIfViolated();

    private static IEnumerable<string?> TablesReadInFullUnder(DbaQueryPlan plan, int parentId)
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
                if (step.Operation is DbaQueryPlanOperation.Scan or DbaQueryPlanOperation.AutomaticIndex)
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
