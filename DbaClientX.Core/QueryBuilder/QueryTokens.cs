using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

/// <summary>Marker interface for <c>WHERE</c> clause tokens.</summary>
public interface IWhereToken { }

/// <summary>Represents a comparison predicate in the <c>WHERE</c> clause.</summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Operator">The comparison operator.</param>
/// <param name="Value">The comparison value.</param>
public sealed record ConditionToken(string Column, string Operator, object Value) : IWhereToken;

/// <summary>Represents a logical operator token, such as <c>AND</c> or <c>OR</c>.</summary>
/// <param name="Operator">The logical operator text.</param>
public sealed record OperatorToken(string Operator) : IWhereToken;

/// <summary>Marks the start of a grouped condition.</summary>
public sealed record GroupStartToken() : IWhereToken;

/// <summary>Marks the end of a grouped condition.</summary>
public sealed record GroupEndToken() : IWhereToken;

/// <summary>Represents an <c>IS NULL</c> predicate.</summary>
/// <param name="Column">The column evaluated for null.</param>
public sealed record NullToken(string Column) : IWhereToken;

/// <summary>Represents an <c>IS NOT NULL</c> predicate.</summary>
/// <param name="Column">The column evaluated for non-null values.</param>
public sealed record NotNullToken(string Column) : IWhereToken;

/// <summary>Represents an <c>IN</c> predicate with literal values.</summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Values">The values to test.</param>
public sealed record InToken(string Column, IReadOnlyList<object> Values) : IWhereToken;

/// <summary>Represents a <c>NOT IN</c> predicate with literal values.</summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Values">The values to exclude.</param>
public sealed record NotInToken(string Column, IReadOnlyList<object> Values) : IWhereToken;

/// <summary>Represents a <c>BETWEEN</c> predicate.</summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Start">The inclusive start value.</param>
/// <param name="End">The inclusive end value.</param>
public sealed record BetweenToken(string Column, object Start, object End) : IWhereToken;

/// <summary>Represents a <c>NOT BETWEEN</c> predicate.</summary>
/// <param name="Column">The column being compared.</param>
/// <param name="Start">The inclusive start value.</param>
/// <param name="End">The inclusive end value.</param>
public sealed record NotBetweenToken(string Column, object Start, object End) : IWhereToken;
