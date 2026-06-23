namespace DBAClientX.SqlServerManagement;

internal sealed class SqlServerTableColumnScriptInfo
{
    public string SchemaName { get; set; } = string.Empty;

    public string TableName { get; set; } = string.Empty;

    public string ColumnName { get; set; } = string.Empty;

    public int Ordinal { get; set; }

    public string DataType { get; set; } = string.Empty;

    public bool IsNullable { get; set; }

    public bool IsIdentity { get; set; }

    public string? IdentitySeed { get; set; }

    public string? IdentityIncrement { get; set; }

    public string? DefaultDefinition { get; set; }

    public string? ComputedDefinition { get; set; }

    public bool IsPersisted { get; set; }
}
