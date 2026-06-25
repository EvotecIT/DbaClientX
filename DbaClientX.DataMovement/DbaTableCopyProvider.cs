namespace DBAClientX.DataMovement;

/// <summary>
/// Identifies a DbaClientX provider that can participate in provider-backed table copy operations.
/// </summary>
public enum DbaTableCopyProvider
{
    /// <summary>Microsoft SQL Server.</summary>
    SqlServer,

    /// <summary>PostgreSQL.</summary>
    PostgreSql,

    /// <summary>MySQL or MariaDB through MySqlConnector.</summary>
    MySql,

    /// <summary>Oracle Database.</summary>
    Oracle,

    /// <summary>SQLite database files or SQLite connection strings.</summary>
    SQLite
}
