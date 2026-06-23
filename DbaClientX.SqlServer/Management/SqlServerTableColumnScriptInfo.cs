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

    public bool IdentityNotForReplication { get; set; }

    public bool IsRowGuidColumn { get; set; }

    public string? DefaultConstraintName { get; set; }

    public string? DefaultDefinition { get; set; }

    public string? ComputedDefinition { get; set; }

    public bool IsPersisted { get; set; }

    public string? GeneratedAlwaysTypeDescription { get; set; }

    public bool IsHidden { get; set; }

    public bool IsSparse { get; set; }

    public string? MaskingFunction { get; set; }

    public int TemporalType { get; set; }

    public string? HistoryTableSchema { get; set; }

    public string? HistoryTableName { get; set; }

    public string? PrimaryKeyName { get; set; }

    public int? PrimaryKeyOrdinal { get; set; }

    public string? PrimaryKeyIndexType { get; set; }

    public bool? PrimaryKeyIsDescending { get; set; }

    public string? UniqueConstraintName { get; set; }

    public int? UniqueConstraintOrdinal { get; set; }

    public string? UniqueConstraintIndexType { get; set; }

    public bool? UniqueConstraintIsDescending { get; set; }

    public string? AdditionalConstraintDefinitions { get; set; }

    public string? PostCreateStatements { get; set; }
}
