namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server instance configuration metadata from sys.configurations.
/// </summary>
public sealed class SqlServerConfigurationInfo
{
    /// <summary>Configuration name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Configured value.</summary>
    public int Value { get; set; }

    /// <summary>Value currently in use.</summary>
    public int ValueInUse { get; set; }

    /// <summary>Minimum allowed value.</summary>
    public int Minimum { get; set; }

    /// <summary>Maximum allowed value.</summary>
    public int Maximum { get; set; }

    /// <summary>True when changes take effect dynamically.</summary>
    public bool IsDynamic { get; set; }

    /// <summary>True when the configuration is marked advanced.</summary>
    public bool IsAdvanced { get; set; }

    /// <summary>Configuration description.</summary>
    public string? Description { get; set; }
}
