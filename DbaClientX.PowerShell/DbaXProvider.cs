namespace DBAClientX.PowerShell;

/// <summary>
/// Identifies a DbaClientX database provider exposed by provider-neutral PowerShell cmdlets.
/// </summary>
public enum DbaXProvider
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
