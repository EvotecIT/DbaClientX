using System;
using System.Linq;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Adds a comparison against a SQL function of one trusted expression and bound argument values.</summary>
    /// <param name="functionName">An unqualified function name containing letters, digits or underscores.</param>
    /// <param name="expression">Trusted SQL for the first argument. Never pass untrusted input.</param>
    /// <param name="op">The comparison operator.</param>
    /// <param name="value">The comparison value.</param>
    /// <param name="arguments">Additional function arguments, captured when the condition is added.</param>
    /// <returns>The current query.</returns>
    /// <remarks>Both the additional arguments and the comparison value become parameters with parameterized compilation.</remarks>
    /// <exception cref="ArgumentException">A name, expression, operator or value is invalid.</exception>
    public Query WhereFunctionRaw(string functionName, string expression, string op, object value, params object[] arguments)
    {
        ValidateString(functionName, nameof(functionName));
        ValidateString(expression, nameof(expression));
        if (!(char.IsLetter(functionName[0]) || functionName[0] == '_') || functionName.Any(c => !(char.IsLetterOrDigit(c) || c == '_')))
            throw new ArgumentException("An unqualified function name is required.", nameof(functionName));
        var normalized = QueryComparisonOperator.Normalize(op, nameof(op));
        if (normalized is "IN" or "NOT IN" || value == null || arguments == null || arguments.Any(a => a == null))
            throw new ArgumentException("A scalar comparison and non-null argument values are required.");
        AddLogicalOperator(null);
        _where.Add(new FunctionConditionToken(functionName, expression, normalized, value, arguments.ToArray()));
        return this;
    }
}
