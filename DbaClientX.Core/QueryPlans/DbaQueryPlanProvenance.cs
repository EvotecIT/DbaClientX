using DBAClientX.QueryBuilder;

namespace DBAClientX.QueryPlans;

/// <summary>Identifies the provider evidence and parameter context of an estimated plan.</summary>
public sealed class DbaQueryPlanProvenance
{
    /// <summary>Creates provenance for a provider's estimated plan.</summary>
    /// <param name="dialect">The database that produced the plan.</param>
    /// <param name="format">The native plan format, such as SHOWPLAN XML.</param>
    /// <param name="parameterMode">How parameters were presented to the planner.</param>
    /// <param name="statementType">The native statement classification, when provided.</param>
    public DbaQueryPlanProvenance(SqlDialect dialect, string format,
        DbaQueryPlanParameterMode parameterMode = DbaQueryPlanParameterMode.Unspecified, string? statementType = null)
    {
        if (!Enum.IsDefined(typeof(SqlDialect), dialect)) throw new ArgumentOutOfRangeException(nameof(dialect));
        if (string.IsNullOrWhiteSpace(format)) throw new ArgumentException("A native plan format is required.", nameof(format));
        if (!Enum.IsDefined(typeof(DbaQueryPlanParameterMode), parameterMode)) throw new ArgumentOutOfRangeException(nameof(parameterMode));
        Dialect = dialect;
        Format = format;
        ParameterMode = parameterMode;
        StatementType = statementType;
    }

    /// <summary>Gets the database that produced the plan.</summary>
    public SqlDialect Dialect { get; }
    /// <summary>Gets the native plan format.</summary>
    public string Format { get; }
    /// <summary>Gets how parameters were presented to the planner.</summary>
    public DbaQueryPlanParameterMode ParameterMode { get; }
    /// <summary>Gets the native statement classification; SQL Server's SELECT WITHOUT QUERY has no relational operators.</summary>
    public string? StatementType { get; }
}

/// <summary>The parameter context used to obtain an estimated plan.</summary>
public enum DbaQueryPlanParameterMode
{
    /// <summary>The caller supplied a plan without recording its parameter context.</summary>
    Unspecified,
    /// <summary>The explained statement has no caller parameters.</summary>
    None,
    /// <summary>Parameters were bound through the provider.</summary>
    BoundValues,
    /// <summary>Parameters were declared as typed local variables without values; estimates are generic.</summary>
    TypedVariables
}
