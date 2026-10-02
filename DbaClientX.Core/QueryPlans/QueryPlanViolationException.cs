namespace DBAClientX.QueryPlans;

/// <summary>
/// Thrown by <see cref="QueryPlanAssert"/> when a plan breaks its rules. Any test framework reports it as a failure;
/// the message names the violations, the statement and the plan.
/// </summary>
public sealed class QueryPlanViolationException : DbaClientXException
{
    /// <summary>Creates the exception for a failed check.</summary>
    /// <param name="result">The failed check.</param>
    public QueryPlanViolationException(QueryPlanCheckResult result)
        : base((result ?? throw new System.ArgumentNullException(nameof(result))).Message)
    {
        Result = result;
    }

    /// <summary>Gets the failed check, with the plan and its violations.</summary>
    public QueryPlanCheckResult Result { get; }
}
