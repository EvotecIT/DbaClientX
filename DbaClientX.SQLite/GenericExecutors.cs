using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX.SQLiteGeneric;

/// <summary>
/// Generic, minimal static entry points for hosts that want to execute raw SQL with parameters.
/// Accepts either a full SQLite connection string or a database file path.
/// </summary>
public static class GenericExecutors
{
    public static Task<int> ExecuteSqlAsync(string connectionStringOrPath, string sql, IDictionary<string, object?>? parameters = null, CancellationToken ct = default)
    {
        var db = ResolveDatabasePath(connectionStringOrPath);
        var cli = new DBAClientX.SQLite();
        return cli.ExecuteNonQueryAsync(db, sql, parameters, cancellationToken: ct);
    }

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
