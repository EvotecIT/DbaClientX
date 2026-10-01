using System;
using System.Collections.Generic;
using System.Linq;

namespace DBAClientX.QueryPlans;

/// <summary>The outcome of <see cref="QueryPlanAssert.Check"/>: the plan and the steps that break the rules.</summary>
public sealed class QueryPlanCheckResult
{
    /// <summary>Creates a result.</summary>
    /// <param name="plan">The checked plan.</param>
    /// <param name="violations">The steps that break the rules.</param>
    public QueryPlanCheckResult(DbaQueryPlan plan, IReadOnlyList<QueryPlanViolation> violations)
    {
        Plan = plan ?? throw new ArgumentNullException(nameof(plan));
        Violations = violations ?? throw new ArgumentNullException(nameof(violations));
    }

    /// <summary>Gets the checked plan.</summary>
    public DbaQueryPlan Plan { get; }

    /// <summary>Gets the steps that break the rules.</summary>
    public IReadOnlyList<QueryPlanViolation> Violations { get; }

    /// <summary>Gets whether the plan follows the rules.</summary>
    public bool IsSuccess => Violations.Count == 0;

    /// <summary>Gets a message naming every violation, the statement and the whole plan, or an empty string on success.</summary>
    public string Message => IsSuccess
        ? string.Empty
        : "The query plan breaks the plan rules:" + Environment.NewLine +
          string.Join(Environment.NewLine, Violations.Select(violation => "  - " + violation)) + Environment.NewLine +
          "SQL:" + Environment.NewLine + Plan.Sql + Environment.NewLine +
          "Plan:" + Environment.NewLine + Plan;

    /// <summary>Throws <see cref="QueryPlanViolationException"/> when the plan breaks the rules.</summary>
    /// <returns>This result, for chaining.</returns>
    /// <exception cref="QueryPlanViolationException">The plan has violations.</exception>
    public QueryPlanCheckResult ThrowIfViolated()
        => IsSuccess ? this : throw new QueryPlanViolationException(this);
}
