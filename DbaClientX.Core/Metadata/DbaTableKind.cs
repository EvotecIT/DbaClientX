namespace DBAClientX.Metadata;

/// <summary>
/// Classifies provider-neutral tabular objects returned by metadata discovery.
/// </summary>
public enum DbaTableKind
{
    /// <summary>A regular table.</summary>
    Table,

    /// <summary>A view or virtual table-like object.</summary>
    View
}
