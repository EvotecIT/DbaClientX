namespace DBAClientX.QueryPlans;

/// <summary>The native estimated JSON schema to interpret.</summary>
public enum MySqlQueryPlanFormat
{
    /// <summary>MySQL EXPLAIN FORMAT=JSON version 1, with a query_block root.</summary>
    MySqlJsonV1,
    /// <summary>MariaDB EXPLAIN FORMAT=JSON, without runtime ANALYZE counters.</summary>
    MariaDbJson
}
