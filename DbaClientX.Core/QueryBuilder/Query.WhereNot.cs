using System;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>
    /// Adds <c>NOT (...)</c> around the conditions that <paramref name="conditions"/> adds, joined with <c>AND</c>.
    /// </summary>
    /// <param name="conditions">Adds one or more conditions to the query it receives, with the usual <c>Where</c>
    /// methods; values stay parameters.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <remarks>
    /// SQL uses three-valued logic: a row whose inner condition is unknown (it compares a NULL) is excluded by both the
    /// condition and its negation. To let such rows match, group the negation with a NULL test, because <c>AND</c>
    /// binds tighter than <c>OR</c>: <c>BeginGroup().WhereNot(q =&gt; ...).OrWhereNull(column).EndGroup()</c>. Without
    /// the group, <c>a AND NOT (b) OR c IS NULL</c> also matches rows that fail <c>a</c>.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="conditions"/> is null.</exception>
    /// <exception cref="ArgumentException">The callback adds no condition, or anything other than conditions.</exception>
    public Query WhereNot(Action<Query> conditions)
        => AddNegatedGroup(conditions, logical: null);

    /// <summary>
    /// Adds <c>NOT (...)</c> around the conditions that <paramref name="conditions"/> adds, joined with <c>OR</c>.
    /// </summary>
    /// <inheritdoc cref="WhereNot(Action{Query})"/>
    public Query OrWhereNot(Action<Query> conditions)
        => AddNegatedGroup(conditions, "OR");

    private Query AddNegatedGroup(Action<Query> conditions, string? logical)
    {
        if (conditions == null)
        {
            throw new ArgumentNullException(nameof(conditions));
        }

        var inner = new Query();
        conditions(inner);
        inner.ValidateConditionGroup(nameof(conditions));

        AddLogicalOperator(logical);
        _where.Add(new NotGroupStartToken());
        _where.AddRange(inner._where);
        _where.Add(new GroupEndToken());
        return this;
    }

    /// <summary>Checks that a query built by a condition callback holds only a complete <c>WHERE</c> expression.</summary>
    private void ValidateConditionGroup(string parameterName)
    {
        if (_where.Count == 0)
        {
            throw new ArgumentException("The condition callback must add at least one condition.", parameterName);
        }

        if (_openGroups != 0)
        {
            throw new ArgumentException("The condition callback left a group open.", parameterName);
        }

        // Every operator must sit between two complete conditions, and every group must hold one.
        IWhereToken? previous = null;
        foreach (var token in _where)
        {
            var afterOpening = previous is null or OperatorToken or GroupStartToken or NotGroupStartToken;
            if ((token is OperatorToken || token is GroupEndToken) && afterOpening)
            {
                throw new ArgumentException("The condition callback added AND/OR or a group without a condition on both sides.", parameterName);
            }

            previous = token;
        }

        if (previous is OperatorToken)
        {
            throw new ArgumentException("The condition callback cannot end with AND/OR.", parameterName);
        }

        if (_select.Count > 0 || _from != null || _fromSubquery != null || _joins.Count > 0 || _insertTable != null ||
            _updateTable != null || _deleteTable != null || _set.Count > 0 || _values.Count > 0 || _orderBy.Count > 0 ||
            _groupBy.Count > 0 || _having.Count > 0 || _limit.HasValue || _offset.HasValue || _useTop ||
            _compoundQueries.Count > 0 || _distinct || _requiresParameterizedCompile || _hasExplicitUpsertUpdateOnly ||
            _upsertUpdateOnly.Count > 0)
        {
            throw new ArgumentException("The condition callback can only add conditions.", parameterName);
        }
    }
}
