namespace DBAClientX;

/// <summary>Controls the scope and bounded diagnostic output of SQL Server CHECKDB.</summary>
public sealed class SqlServerIntegrityCheckOptions
{
    /// <summary>Runs physical page/allocation checks only. False performs the default full logical checks.</summary>
    public bool PhysicalOnly { get; set; }

    /// <summary>Includes SQL Server's diagnostic message text, which can contain object names or data details.</summary>
    public bool IncludeDiagnosticMessages { get; set; }

    /// <summary>Maximum diagnostic records retained. Additional records are counted without being stored.</summary>
    public int MaxIssues { get; set; } = 1000;
}
