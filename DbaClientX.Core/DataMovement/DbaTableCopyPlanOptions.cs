using DBAClientX.Metadata;

namespace DBAClientX.DataMovement;

/// <summary>
/// Controls how metadata is converted into provider-neutral table-copy definitions.
/// </summary>
public sealed class DbaTableCopyPlanOptions
{
    /// <summary>Restricts source tables to a schema. When null, all provided source schemas are considered.</summary>
    public string? SourceSchema { get; init; }

    /// <summary>Overrides destination schema for generated destination names unless a table mapping already includes a schema.</summary>
    public string? DestinationSchema { get; init; }

    /// <summary>When true, view metadata can produce copy definitions. Regular tables are always included.</summary>
    public bool IncludeViews { get; init; }

    /// <summary>Maps source table names to destination table names. Keys may be unqualified or schema-qualified.</summary>
    public IReadOnlyDictionary<string, string>? TableMappings { get; init; }

    /// <summary>Global source-to-destination column mappings applied to every table unless overridden by table-specific mappings.</summary>
    public IReadOnlyDictionary<string, string>? ColumnMappings { get; init; }

    /// <summary>Table-specific source-to-destination column mappings. Table keys may be unqualified or schema-qualified.</summary>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<string, string>>? TableColumnMappings { get; init; }

    /// <summary>Global source or destination column names excluded from every generated definition.</summary>
    public IReadOnlyCollection<string>? ExcludedColumns { get; init; }

    /// <summary>Table-specific source or destination column names excluded from generated definitions.</summary>
    public IReadOnlyDictionary<string, IReadOnlyCollection<string>>? TableExcludedColumns { get; init; }

    /// <summary>Global column type conversions applied before destination writes.</summary>
    public IReadOnlyDictionary<string, DbaTableCopyColumnType>? ColumnTypeConversions { get; init; }

    /// <summary>Table-specific column type conversions applied before destination writes. Table keys may be unqualified or schema-qualified.</summary>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<string, DbaTableCopyColumnType>>? TableColumnTypeConversions { get; init; }

    /// <summary>Global source-side shaping applied to every generated definition unless overridden for a table.</summary>
    public DbaTableCopySourceOptions? SourceOptions { get; init; }

    /// <summary>Table-specific source-side shaping. Table keys may be unqualified or schema-qualified.</summary>
    public IReadOnlyDictionary<string, DbaTableCopySourceOptions>? TableSourceOptions { get; init; }

    /// <summary>Explicit order columns for paged reads. Table keys may be unqualified or schema-qualified.</summary>
    public IReadOnlyDictionary<string, IReadOnlyList<string>>? OrderByColumns { get; init; }

    /// <summary>When true, source columns reported as generated/computed are excluded from destination pages.</summary>
    public bool ExcludeSourceGeneratedColumns { get; init; } = true;

    /// <summary>When true, destination columns reported as generated/computed are excluded from destination pages.</summary>
    public bool ExcludeDestinationGeneratedColumns { get; init; } = true;

    /// <summary>When true, destination columns reported as identity/auto-incrementing are excluded from destination pages.</summary>
    public bool ExcludeDestinationIdentityColumns { get; init; }

    /// <summary>When destination column metadata is supplied, exclude source columns that do not map to a destination column.</summary>
    public bool MatchDestinationColumns { get; init; } = true;

    /// <summary>Optional predicate for domain-specific table filtering before definitions are created.</summary>
    public Func<DbaTableInfo, bool>? TablePredicate { get; init; }
}
