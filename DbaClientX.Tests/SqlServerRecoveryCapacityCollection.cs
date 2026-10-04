namespace DbaClientX.Tests;

/// <summary>Runs large native recovery probes after other collections release their temporary resources.</summary>
/// <remarks>Each probe checks current volume space after entering this serial collection. This avoids
/// competing allocations within a test run; it is not a reservation against other processes.</remarks>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class SqlServerRecoveryCapacityCollection
{
    /// <summary>The collection for local native recovery capacity and workload qualification.</summary>
    public const string Name = "SQL Server recovery capacity";
}
