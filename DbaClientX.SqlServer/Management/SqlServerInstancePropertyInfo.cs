namespace DBAClientX.SqlServerManagement;

/// <summary>
/// SQL Server instance property metadata returned by SERVERPROPERTY.
/// </summary>
public sealed class SqlServerInstancePropertyInfo
{
    /// <summary>Property name.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Property value rendered as text.</summary>
    public string? Value { get; set; }
}
