using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX.SQLiteGeneric;

/// <summary>
/// Provides reflection-friendly helpers that forward to <see cref="DBAClientX.SQLite"/> using either a SQLite connection string
/// or a file path. The methods are designed for scenarios where dependency injection or direct instantiation of the provider is
/// impractical, such as in scripting environments.
/// </summary>
public static class GenericExecutors
{
    /// <summary>
    /// Executes a parameterized SQL statement against the provided database.
    /// </summary>
    /// <param name="connectionStringOrPath">Either a full SQLite connection string or a database file path.</param>
    /// <param name="sql">SQL text to execute.</param>
    /// <param name="parameters">Optional map containing parameter names and values.</param>
    /// <param name="ct">Token used to cancel the operation.</param>
    /// <returns>A task producing the number of rows affected by the command.</returns>
    /// <remarks>
    /// The helper instantiates a new <see cref="DBAClientX.SQLite"/> instance for each invocation, making it suitable for
    /// dynamic scenarios where holding onto state is difficult. It leverages <see cref="DBAClientX.SQLite.ExecuteNonQueryAsync"/>
    /// internally, meaning that standard validation and exception behaviors are preserved.
    /// </remarks>
    public static Task<int> ExecuteSqlAsync(string connectionStringOrPath, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var db = ResolveDatabasePath(connectionStringOrPath);
        var cli = new DBAClientX.SQLite();
        return cli.ExecuteNonQueryAsync(db, sql, parameters, cancellationToken: ct);
    }

    /// <summary>
    /// Not supported because SQLite does not implement stored procedure semantics.
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
