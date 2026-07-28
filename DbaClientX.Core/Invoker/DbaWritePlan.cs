using System.Collections.Generic;

namespace DBAClientX.Invoker;

/// <summary>
/// Contains provider-specific SQL and the logical-to-parameter mapping for an insert or upsert.
/// </summary>
/// <param name="Sql">Compiled provider-specific SQL.</param>
/// <param name="ParameterMap">Logical source member to provider parameter mapping.</param>
/// <param name="Columns">Ordered destination columns included in the write.</param>
public sealed record DbaWritePlan(
    string Sql,
    IReadOnlyDictionary<string, string> ParameterMap,
    IReadOnlyList<string> Columns);
