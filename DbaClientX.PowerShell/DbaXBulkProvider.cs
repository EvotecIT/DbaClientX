namespace DBAClientX.PowerShell;

/// <summary>
/// Identifies the database provider used by bulk table-data cmdlets.
/// </summary>
public enum DbaXBulkProvider
{
    /// <summary>SQL Server provider.</summary>
    SqlServer,

    /// <summary>PostgreSQL provider.</summary>
    PostgreSql,

    /// <summary>MySQL provider.</summary>
    MySql,

    /// <summary>Oracle provider.</summary>
    Oracle,

    /// <summary>SQLite provider.</summary>
    SQLite
}
