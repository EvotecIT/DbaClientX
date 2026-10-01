using System.Globalization;

namespace DBAClientX.QueryPlans;

/// <summary>Why a query plan step breaks <see cref="QueryPlanRules"/>.</summary>
public enum QueryPlanViolationKind
{
    /// <summary>The step reads every row of a large table.</summary>
    FullScan,

    /// <summary>The step sorts or groups rows of a large table in a temporary B-tree.</summary>
    TempBTree,

    /// <summary>The step builds a statement-only index over a large table.</summary>
    AutomaticIndex,

    /// <summary>
    /// A named large table appears in no step under its name: the statement does not read it, or the plan names it in
    /// a way the rules cannot match (an alias used for two tables, a typo). <see cref="QueryPlanViolation.Step"/> is null.
    /// </summary>
    TableNotInPlan,

    /// <summary>
    /// The step searches an index of a large table but reads much of it: more than
    /// <see cref="QueryPlanRules.WideSearchFraction"/> of its rows by the statistics, or a range open on one side that no
    /// <c>LIMIT</c> stops (see <see cref="QueryPlanRules.ForbidWideSearches"/>).
    /// </summary>
    WideSearch
}

/// <summary>One step of a plan that breaks <see cref="QueryPlanRules"/>.</summary>
public sealed class QueryPlanViolation
{
    /// <summary>Creates a violation.</summary>
    /// <param name="kind">Why the step breaks the rules.</param>
    /// <param name="table">The large table involved.</param>
    /// <param name="step">The offending step, or null for <see cref="QueryPlanViolationKind.TableNotInPlan"/>.</param>
    public QueryPlanViolation(QueryPlanViolationKind kind, string table, DbaQueryPlanStep? step)
    {
        Kind = kind;
        Table = table;
        Step = step;
    }

    /// <summary>Gets why the step breaks the rules.</summary>
    public QueryPlanViolationKind Kind { get; }

    /// <summary>Gets the large table involved.</summary>
    public string Table { get; }

    /// <summary>Gets the offending step, or null for <see cref="QueryPlanViolationKind.TableNotInPlan"/>.</summary>
    public DbaQueryPlanStep? Step { get; }

    /// <inheritdoc />
    public override string ToString() => Kind switch
    {
        QueryPlanViolationKind.FullScan => $"Full scan of '{Table}': {Step?.Detail}",
        QueryPlanViolationKind.TempBTree => $"Temporary B-tree over rows of '{Table}': {Step?.Detail}",
        QueryPlanViolationKind.AutomaticIndex => $"Automatic index built over '{Table}': {Step?.Detail}",
        QueryPlanViolationKind.WideSearch when Step?.EstimatedRows != null && Step.TableRows is > 0 && !Step.IsOpenEndedRange =>
            $"Wide search of '{Table}', about {Step.EstimatedRows.Value.ToString("N0", CultureInfo.InvariantCulture)} of {Step.TableRows.Value.ToString("N0", CultureInfo.InvariantCulture)} rows by the statistics: {Step.Detail}",
        QueryPlanViolationKind.WideSearch => $"Wide search of '{Table}', a range open on one side that no LIMIT stops: {Step?.Detail}",
        _ => $"Table '{Table}' does not appear in the plan under that name; check the name, or set RequireLargeTablesInPlan to false for statements that do not read it"
    };
}
