using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace DBAClientX.PostgreSqlGeneric;

/// <summary>
/// Generic, reflection-friendly façade for executing SQL or stored procedures via a PostgreSQL connection string.
/// Internally forwards to <see cref="DBAClientX.PostgreSql"/>.
/// </summary>
public static class GenericExecutors
{
    /// <summary>Executes a parameterized SQL statement.</summary>
    /// <param name="connectionString">Npgsql connection string.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static Task<int> ExecuteSqlAsync(string connectionString, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new NpgsqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.PostgreSql();
        return cli.ExecuteNonQueryAsync(b.Host, b.Database, b.Username, b.Password, sql, parameters, cancellationToken: ct);
    }

    /// <summary>Executes a stored procedure.</summary>
    /// <param name="connectionString">Npgsql connection string.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Zero. This façade returns 0 to keep cross-provider signatures uniform.</returns>
    public static async Task<int> ExecuteProcedureAsync(string connectionString, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var b = new NpgsqlConnectionStringBuilder(connectionString);
        var cli = new DBAClientX.PostgreSql();
        await cli.ExecuteStoredProcedureAsync(b.Host, b.Database, b.Username, b.Password, procedure, parameters, cancellationToken: ct).ConfigureAwait(false);
        return 0;
    }
}
