namespace DBAClientX.QueryPlans;

/// <summary>One step of a <see cref="DbaQueryPlan"/>.</summary>
public sealed class DbaQueryPlanStep
{
    /// <summary>Creates a step.</summary>
    /// <param name="id">The step identifier the database reported.</param>
    /// <param name="parentId">The identifier of the parent step, or 0 for a top-level step.</param>
    /// <param name="detail">The database's own text for the step.</param>
    /// <param name="operation">What the step does.</param>
    /// <param name="table">The table (or alias target) the step reads, when it reads one.</param>
    /// <param name="index">The index the step uses, when it uses a named one.</param>
    /// <param name="isCoveringIndex">Whether the index holds every column the step needs.</param>
    /// <param name="tempBTreePurpose">For <see cref="DbaQueryPlanOperation.TempBTree"/>, what the temporary B-tree sorts or groups (for example <c>ORDER BY</c>).</param>
    /// <param name="alias">The alias the statement gave the table, when the database reported the alias.</param>
    public DbaQueryPlanStep(
        int id,
        int parentId,
        string detail,
        DbaQueryPlanOperation operation,
        string? table = null,
        string? index = null,
        bool isCoveringIndex = false,
        string? tempBTreePurpose = null,
        string? alias = null)
    {
        Alias = alias;
        Id = id;
        ParentId = parentId;
        Detail = detail ?? string.Empty;
        Operation = operation;
        Table = table;
        Index = index;
        IsCoveringIndex = isCoveringIndex;
        TempBTreePurpose = tempBTreePurpose;
    }

    /// <summary>Gets the step identifier the database reported.</summary>
    public int Id { get; }

    /// <summary>Gets the identifier of the parent step, or 0 for a top-level step.</summary>
    public int ParentId { get; }

    /// <summary>Gets the database's own text for the step.</summary>
    public string Detail { get; }

    /// <summary>Gets what the step does.</summary>
    public DbaQueryPlanOperation Operation { get; }

    /// <summary>Gets the table the step reads, when it reads one (resolved from an alias where the statement shows it).</summary>
    public string? Table { get; }

    /// <summary>Gets the alias the statement gave the table, when the database reported the alias instead of the name.</summary>
    public string? Alias { get; }

    /// <summary>Gets the index the step uses, when it uses a named one (for SQLite also <c>INTEGER PRIMARY KEY</c>).</summary>
    public string? Index { get; }

    /// <summary>Gets whether the index holds every column the step needs.</summary>
    public bool IsCoveringIndex { get; }

    /// <summary>Gets what a temporary B-tree is for (<c>ORDER BY</c>, <c>GROUP BY</c>, <c>DISTINCT</c>), or <see langword="null"/>.</summary>
    public string? TempBTreePurpose { get; }

    /// <inheritdoc />
    public override string ToString() => Detail;
}
