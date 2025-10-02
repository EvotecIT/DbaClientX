namespace DBAClientX.QueryBuilder;

/// <summary>
/// Supported SQL dialects for query compilation.
/// </summary>
public enum SqlDialect
{
    /// <summary>
    /// Microsoft SQL Server.
    /// </summary>
    SqlServer,

    /// <summary>
    /// PostgreSQL.
    /// </summary>
    PostgreSql,

    /// <summary>
    /// MySQL.
    /// </summary>
    MySql,

    /// <summary>
    /// SQLite.
    /// </summary>
    SQLite,

    /// <summary>
    /// Oracle Database.
    /// </summary>
    Oracle
}
