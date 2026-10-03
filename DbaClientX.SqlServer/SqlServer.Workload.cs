using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.SqlServerMonitoring;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <summary>Reads Query Store configuration for the target database without changing capture policy or returning query text.</summary>
    /// <param name="target">Database and authentication settings.</param>
    /// <param name="cancellationToken">Cancels the database read.</param>
    /// <returns>Configured and actual state, storage and read-only reasons.</returns>
    /// <remarks>Requires SQL Server 2016 or later and the applicable database-state/performance-state permission.</remarks>
    /// <exception cref="NotSupportedException">The server version or database does not support Query Store, including master and tempdb.</exception>
    /// <exception cref="System.Data.DataException">Query Store configuration metadata is unavailable; this does not establish that it is OFF.</exception>
    public virtual async Task<SqlServerQueryStoreState> GetQueryStoreStateAsync(
        SqlServerMonitoringTarget target, CancellationToken cancellationToken = default)
    {
        IReadOnlyList<SqlServerQueryStoreState> rows;
        try
        {
            rows = await ReadRowsAsync(target, QueryStoreStateQuery,
                SqlServerWorkloadMappers.MapQueryStoreState, false, cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException exception) when (exception.Number == 208)
        {
            throw new NotSupportedException("Query Store configuration is unavailable on this SQL Server version.", exception);
        }

        if (rows.Count != 1)
            throw new System.Data.DataException("Query Store configuration metadata is unavailable for this database or caller.");
        return rows[0];
    }

    /// <summary>Reads bounded rowstore table-index usage and statistics for the target database without scanning table contents.</summary>
    /// <param name="target">Database and authentication settings.</param>
    /// <param name="maximumRows">Maximum retained indexes, between 1 and 10,000; additional rows are reported by the truncation flag.</param>
    /// <param name="cancellationToken">Cancels the database read.</param>
    /// <returns>Nullable usage/statistics, restart context and explicit truncation.</returns>
    /// <remarks>
    /// Requires SQL Server 2014 or later and the applicable server-state/performance-state permission.
    /// Catalog visibility and statistics properties remain permission-dependent. Missing or zero counters do not
    /// establish that an index can be removed; primary-key and uniqueness roles are retained for assessment.
    /// </remarks>
    public virtual async Task<SqlServerIndexUsageSnapshot> GetIndexUsageAsync(
        SqlServerMonitoringTarget target, int maximumRows = 500, CancellationToken cancellationToken = default)
    {
        ValidateMonitoringTarget(target);
        ValidateIndexUsageLimit(maximumRows);
        var rows = await ReadRowsAsync(target, IndexUsageQuery, SqlServerWorkloadMappers.MapIndexUsage,
            false, cancellationToken, new Dictionary<string, object?> { ["@maximumRows"] = maximumRows + 1 }).ConfigureAwait(false);
        return new SqlServerIndexUsageSnapshot(target.Database, Array.AsReadOnly(rows.Take(maximumRows).Select(row => row.Index).ToArray()),
            rows.Count > maximumRows, rows.Count == 0 ? null : rows[0].ServerStartTime);
    }

    private static void ValidateIndexUsageLimit(int maximumRows)
    {
        if (maximumRows < 1 || maximumRows > 10000)
            throw new ArgumentOutOfRangeException(nameof(maximumRows), "Index usage row limit must be between 1 and 10,000.");
    }
}
