namespace DBAClientX.QueryPlans;

/// <summary>What a <see cref="DbaQueryPlanStep"/> does.</summary>
public enum DbaQueryPlanOperation
{
    /// <summary>A step the parser does not classify (subquery headers, compound queries, co-routines and similar).</summary>
    Other,

    /// <summary>Reads every row of a table, or every entry of one of its indexes in order.</summary>
    Scan,

    /// <summary>Reads only the rows an index or the primary key finds.</summary>
    Search,

    /// <summary>Builds an index for this statement only (SQLite's automatic index), which means no stored index fits.</summary>
    AutomaticIndex,

    /// <summary>Sorts or groups rows in a temporary B-tree because no index gives the order.</summary>
    TempBTree,

    /// <summary>Reads a virtual table (for example FTS5) through its own index.</summary>
    VirtualTable
}
