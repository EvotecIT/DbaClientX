using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX.MySqlGeneric;

/// <summary>
/// Generic, reflection-friendly façade for executing SQL or stored procedures via a MySQL connection string.
/// Internally forwards to <see cref="DBAClientX.MySql"/>.
/// </summary>
public static class GenericExecutors
{
    internal static Func<DBAClientX.MySql> ClientFactory { get; set; } = static () => new DBAClientX.MySql();

    /// <summary>Executes a parameterized SQL statement.</summary>
    /// <param name="connectionString">MySqlConnector connection string.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static async Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        using var cli = ClientFactory();
        return await cli.ExecuteNonQueryAsync(connectionString, sql, parameters, cancellationToken: ct).ConfigureAwait(false);
    }

    /// <summary>Executes a stored procedure.</summary>
    /// <param name="connectionString">MySqlConnector connection string.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Zero. This façade returns 0 to keep cross-provider signatures uniform.</returns>
    public static async Task<int> ExecuteProcedureAsync(string connectionString, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        using var cli = ClientFactory();
        await cli.ExecuteStoredProcedureAsync(connectionString, procedure, parameters, cancellationToken: ct).ConfigureAwait(false);
        return 0;
    }
}
