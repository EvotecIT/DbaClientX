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
    /// <param name="isOpenEndedRange">For a <see cref="DbaQueryPlanOperation.Search"/>, whether the index is searched by a range
    /// bounded on one side only, with no equality before it (<c>CompletedUtcMs &lt; ?</c>).</param>
    /// <param name="estimatedRows">The rows the step reads according to the database's statistics, when known.</param>
    /// <param name="tableRows">The rows of the step's table according to the database's statistics, when known.</param>
    public DbaQueryPlanStep(
        int id,
        int parentId,
        string detail,
        DbaQueryPlanOperation operation,
        string? table = null,
        string? index = null,
        bool isCoveringIndex = false,
        string? tempBTreePurpose = null,
        string? alias = null,
        bool isOpenEndedRange = false,
        long? estimatedRows = null,
        long? tableRows = null)
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
        IsOpenEndedRange = isOpenEndedRange;
        EstimatedRows = estimatedRows;
        TableRows = tableRows;
    }

    /// <summary>Creates a native provider step with precise estimates and separately identified schema/database.</summary>
    /// <param name="id">The native operator identifier.</param>
    /// <param name="parentId">The parent's identifier, or -1 for a native root.</param>
    /// <param name="detail">The native operation text.</param>
    /// <param name="operation">The access method; a scan does not imply full traversal.</param>
    /// <param name="estimates">Native estimates; missing values remain unknown.</param>
    /// <param name="table">The provider's table name, without SQL alias inference.</param>
    /// <param name="index">The provider's index name.</param>
    /// <param name="alias">The provider's alias.</param>
    /// <param name="schema">The provider's schema name.</param>
    /// <param name="database">The provider's database name.</param>
    public DbaQueryPlanStep(int id, int parentId, string detail, DbaQueryPlanOperation operation,
        DbaQueryPlanEstimates estimates, string? table, string? index, string? alias,
        string? schema, string? database)
        : this(id, parentId, detail, operation, table, index, alias: alias)
    {
        Estimates = estimates ?? throw new ArgumentNullException(nameof(estimates));
        Schema = schema;
        Database = database;
    }

    /// <summary>Gets native optimizer estimates, or null for legacy SQLite statistics.</summary>
    public DbaQueryPlanEstimates? Estimates { get; private set; }
    /// <summary>Gets the native schema name, separate from the table name.</summary>
    public string? Schema { get; private set; }
    /// <summary>Gets the native database name, separate from the table name.</summary>
    public string? Database { get; private set; }

    /// <summary>Gets the step identifier the database reported.</summary>
    public int Id { get; }

    /// <summary>Gets the parent identifier; SQLite top-level steps use 0, and native roots use -1.</summary>
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

    /// <summary>Gets SQLite's covering-index flag. Native provider readers do not infer coverage from access labels.</summary>
    public bool IsCoveringIndex { get; }

    /// <summary>Gets what a temporary B-tree is for (<c>ORDER BY</c>, <c>GROUP BY</c>, <c>DISTINCT</c>), or <see langword="null"/>.</summary>
    public string? TempBTreePurpose { get; }

    /// <summary>
    /// Gets whether the step searches an index by a range bounded on one side only, with no equality before it
    /// (<c>CompletedUtcMs &lt; ?</c>): it reads from one end of the index to the bound, which can be no rows or all of
    /// them. The plan does not show which.
    /// </summary>
    public bool IsOpenEndedRange { get; }

    /// <summary>
    /// Gets the rows the step reads according to the database's statistics, or <see langword="null"/> when they are not
    /// known (no statistics, or a range whose width the statistics cannot tell). For a search that also narrows by a
    /// range, it is the rows its equality constraints match, an upper bound.
    /// </summary>
    public long? EstimatedRows { get; }

    /// <summary>Gets the rows of the step's table according to the database's statistics, or <see langword="null"/> when not known.</summary>
    public long? TableRows { get; }

    /// <summary>
    /// Gets the columns the step's index search constrains with their operators (<c>(a=? AND b&gt;?)</c> gives
    /// <c>a =</c> and <c>b &gt;</c>; the rowid also under its alias column's name), when the provider tells; rules compare
    /// them with the conditions of the statement to know whether the index serves them.
    /// </summary>
    internal IReadOnlyList<(string Column, string Operator)>? IndexConstraintColumns { get; private set; }

    /// <summary>
    /// Gets, for each key column of the step's index, the average rows that share one value of the key up to and
    /// including that column, from the statistics; null when not known.
    /// </summary>
    internal IReadOnlyList<(string Column, long Rows)>? IndexRowsPerKey { get; private set; }

    /// <summary>Returns a copy of this step with another table, alias, operation and estimate.</summary>
    internal DbaQueryPlanStep With(
        string? table,
        string? alias,
        DbaQueryPlanOperation operation,
        long? estimatedRows,
        long? tableRows,
        bool? isOpenEndedRange = null,
        IReadOnlyList<(string Column, string Operator)>? indexConstraintColumns = null,
        IReadOnlyList<(string Column, long Rows)>? indexRowsPerKey = null)
        => new(Id, ParentId, Detail, operation, table, Index, IsCoveringIndex, TempBTreePurpose, alias, isOpenEndedRange ?? IsOpenEndedRange, estimatedRows, tableRows)
        {
            IndexConstraintColumns = indexConstraintColumns ?? IndexConstraintColumns,
            IndexRowsPerKey = indexRowsPerKey ?? IndexRowsPerKey,
            Estimates = Estimates,
            Schema = Schema,
            Database = Database
        };

    /// <inheritdoc />
    public override string ToString() => Detail;
}
