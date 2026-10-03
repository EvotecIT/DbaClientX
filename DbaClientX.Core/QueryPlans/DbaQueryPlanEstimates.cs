namespace DBAClientX.QueryPlans;

/// <summary>Native optimizer estimates, with output, access effort and cardinality kept separate.</summary>
/// <remarks>These are estimates, not observed counts. Missing values remain unknown. Cost units are provider-specific.</remarks>
public sealed class DbaQueryPlanEstimates
{
    /// <summary>Creates native estimates without rounding fractional row counts.</summary>
    /// <param name="outputRows">Estimated rows emitted by this operator, per execution.</param>
    /// <param name="rowsRead">Estimated rows accessed before residual filtering, per execution.</param>
    /// <param name="tableRows">Estimated cardinality of the accessed table.</param>
    /// <param name="subtreeCost">The provider's estimated cost of this operator and its subtree.</param>
    public DbaQueryPlanEstimates(double? outputRows = null, double? rowsRead = null,
        double? tableRows = null, double? subtreeCost = null)
    {
        OutputRows = Validate(outputRows, nameof(outputRows));
        RowsRead = Validate(rowsRead, nameof(rowsRead));
        TableRows = Validate(tableRows, nameof(tableRows));
        SubtreeCost = Validate(subtreeCost, nameof(subtreeCost));
    }

    /// <summary>Gets estimated output rows per operator execution, or null when unavailable.</summary>
    public double? OutputRows { get; }
    /// <summary>Gets estimated rows accessed per operator execution before residual filtering, or null.</summary>
    public double? RowsRead { get; }
    /// <summary>Gets estimated table cardinality, or null when unavailable.</summary>
    public double? TableRows { get; }
    /// <summary>Gets native estimated subtree cost; units are meaningful only within the provider.</summary>
    public double? SubtreeCost { get; }

    private static double? Validate(double? value, string name)
    {
        if (value.HasValue && (double.IsNaN(value.Value) || double.IsInfinity(value.Value) || value.Value < 0))
            throw new ArgumentOutOfRangeException(name, "An estimate must be finite and non-negative.");
        return value;
    }
}
