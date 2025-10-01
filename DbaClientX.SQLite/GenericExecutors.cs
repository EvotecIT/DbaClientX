using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX.SQLiteGeneric;

/// <summary>
/// Generic, reflection-friendly façade for executing raw SQL with parameters using SQLite.
/// Accepts either a full SQLite connection string or a database file path.
/// Internally forwards to <see cref="DBAClientX.SQLite"/>.
/// </summary>
public static class GenericExecutors
{
    /// <summary>
    /// Executes a parameterized SQL statement.
    /// </summary>
    /// <param name="connectionStringOrPath">Connection string or database file path.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Parameter name/value map.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Number of affected rows.</returns>
    public static Task<int> ExecuteSqlAsync(string connectionStringOrPath, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var db = ResolveDatabasePath(connectionStringOrPath);
        var cli = new DBAClientX.SQLite();
        return cli.ExecuteNonQueryAsync(db, sql, parameters, cancellationToken: ct);
    }

    /// <summary>
    /// Not supported. SQLite does not support stored procedures.
    /// </summary>
    /// <exception cref="NotSupportedException">Always thrown.</exception>
    public static Task<int> ExecuteProcedureAsync(string connectionStringOrPath, string procedure, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
        => Task.FromException<int>(new NotSupportedException("SQLite does not support stored procedures."));

    private static string ResolveDatabasePath(string connectionStringOrPath)
    {
        if (connectionStringOrPath.IndexOf('=') >= 0)
        {
            var b = new SqliteConnectionStringBuilder(connectionStringOrPath);
            return b.DataSource;
        }
        return connectionStringOrPath;
    }
}
