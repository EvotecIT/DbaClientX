namespace DBAClientX.SqlServerManagement;

/// <summary>
/// Column mapping used by a SQL Server table copy or sync plan.
/// </summary>
public sealed class SqlServerTableCopyColumnInfo
{
    /// <summary>Source column name.</summary>
    public string SourceColumn { get; set; } = string.Empty;

    /// <summary>Destination column name.</summary>
    public string DestinationColumn { get; set; } = string.Empty;

    /// <summary>Provider-specific data type from metadata discovery.</summary>
    public string DataType { get; set; } = string.Empty;

    /// <summary>True when the source column is nullable.</summary>
    public bool? IsNullable { get; set; }

    /// <summary>True when the column participates in the inferred key.</summary>
    public bool IsKey { get; set; }

    /// <summary>Parameter placeholder used by destination command templates.</summary>
    public string ParameterName { get; set; } = string.Empty;
}
