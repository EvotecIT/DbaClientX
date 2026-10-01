namespace DBAClientX.QueryPlans;

/// <summary>The kind of predicate <see cref="SqlSargabilityAnalyzer"/> reports.</summary>
public enum SqlSargabilityFindingKind
{
    /// <summary>A function wraps a column in a <c>WHERE</c> or <c>ON</c> condition (<c>LOWER(Name) = @p</c>), so an index on the column cannot serve it.</summary>
    FunctionOnColumn,

    /// <summary>A <c>LIKE</c> or <c>GLOB</c> pattern starts with a wildcard (<c>LIKE '%x'</c>), so an index cannot narrow it.</summary>
    LeadingWildcard
}

/// <summary>A predicate in SQL text that an index probably cannot serve.</summary>
public sealed class SqlSargabilityFinding
{
    /// <summary>Creates a finding.</summary>
    /// <param name="kind">The kind of predicate.</param>
    /// <param name="position">The character offset in the SQL text where the predicate starts.</param>
    /// <param name="text">The offending SQL fragment.</param>
    /// <param name="message">A sentence describing the problem.</param>
    public SqlSargabilityFinding(SqlSargabilityFindingKind kind, int position, string text, string message)
    {
        Kind = kind;
        Position = position;
        Text = text;
        Message = message;
    }

    /// <summary>Gets the kind of predicate.</summary>
    public SqlSargabilityFindingKind Kind { get; }

    /// <summary>Gets the character offset in the SQL text where the predicate starts.</summary>
    public int Position { get; }

    /// <summary>Gets the offending SQL fragment.</summary>
    public string Text { get; }

    /// <summary>Gets a sentence describing the problem.</summary>
    public string Message { get; }

    /// <inheritdoc />
    public override string ToString() => $"{Message} (at {Position}: {Text})";
}
