namespace DBAClientX.PowerShell;

/// <summary>Gets database metadata without requiring SQL Server Management Objects.</summary>
/// <para>Returns provider-neutral metadata for databases, tables/views, columns, indexes, foreign keys, and routines using native catalog queries from the selected provider.</para>
/// <example>
/// <summary>List SQL Server tables.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXMetadata -Provider SqlServer -Type Table -ConnectionString 'Server=.;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True'</code>
/// <para>Lists tables and views visible through the supplied SQL Server connection.</para>
/// </example>
/// <example>
/// <summary>List SQLite columns for one table.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXMetadata -Provider SQLite -Type Column -ConnectionString '.\app.db' -Table Users</code>
/// <para>Returns column metadata for the Users table in the SQLite database file.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXMetadata")]
[CmdletBinding()]
public sealed class CmdletGetDbaXMetadata : AsyncPSCmdlet
{
    /// <summary>Specifies the database provider.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Selects the metadata type to return.</summary>
    [Parameter(Mandatory = true, Position = 1)]
    public DbaXMetadataType Type { get; set; }

    /// <summary>Specifies a provider connection string, or a SQLite database path when <see cref="Provider"/> is SQLite.</summary>
    [Parameter(Mandatory = true, Position = 2)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Optional schema or owner filter where the selected provider supports schemas.</summary>
    [Parameter(Mandatory = false)]
    public string? Schema { get; set; }

    /// <summary>Optional table filter for column, index, and foreign key metadata.</summary>
    [Parameter(Mandatory = false)]
    public string? Table { get; set; }

    /// <summary>Excludes views when requesting table metadata.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter ExcludeViews { get; set; }

    /// <summary>Processes the metadata request.</summary>
    protected override async Task ProcessRecordAsync()
    {
        await Task.Yield();
        var result = Provider switch
        {
            DbaXProvider.SqlServer => GetSqlServerMetadata(),
            DbaXProvider.PostgreSql => GetPostgreSqlMetadata(),
            DbaXProvider.MySql => GetMySqlMetadata(),
            DbaXProvider.Oracle => GetOracleMetadata(),
            DbaXProvider.SQLite => GetSQLiteMetadata(),
            _ => throw new NotSupportedException($"Provider '{Provider}' is not supported.")
        };

        WriteObject(result, enumerateCollection: true);
    }

    private object GetSqlServerMetadata()
    {
        using var client = new DBAClientX.SqlServer();
        return Type switch
        {
            DbaXMetadataType.Database => client.GetDatabases(ConnectionString),
            DbaXMetadataType.Table => client.GetTables(ConnectionString, Schema, !ExcludeViews.IsPresent),
            DbaXMetadataType.Column => client.GetColumns(ConnectionString, Schema, Table),
            DbaXMetadataType.Index => client.GetIndexes(ConnectionString, Schema, Table),
            DbaXMetadataType.ForeignKey => client.GetForeignKeys(ConnectionString, Schema, Table),
            DbaXMetadataType.Routine => client.GetRoutines(ConnectionString, Schema),
            _ => throw new NotSupportedException($"Metadata type '{Type}' is not supported.")
        };
    }

    private object GetPostgreSqlMetadata()
    {
        using var client = new DBAClientX.PostgreSql();
        return Type switch
        {
            DbaXMetadataType.Database => client.GetDatabases(ConnectionString),
            DbaXMetadataType.Table => client.GetTables(ConnectionString, Schema, !ExcludeViews.IsPresent),
            DbaXMetadataType.Column => client.GetColumns(ConnectionString, Schema, Table),
            DbaXMetadataType.Index => client.GetIndexes(ConnectionString, Schema, Table),
            DbaXMetadataType.ForeignKey => client.GetForeignKeys(ConnectionString, Schema, Table),
            DbaXMetadataType.Routine => client.GetRoutines(ConnectionString, Schema),
            _ => throw new NotSupportedException($"Metadata type '{Type}' is not supported.")
        };
    }

    private object GetMySqlMetadata()
    {
        using var client = new DBAClientX.MySql();
        return Type switch
        {
            DbaXMetadataType.Database => client.GetDatabases(ConnectionString),
            DbaXMetadataType.Table => client.GetTables(ConnectionString, Schema, !ExcludeViews.IsPresent),
            DbaXMetadataType.Column => client.GetColumns(ConnectionString, Schema, Table),
            DbaXMetadataType.Index => client.GetIndexes(ConnectionString, Schema, Table),
            DbaXMetadataType.ForeignKey => client.GetForeignKeys(ConnectionString, Schema, Table),
            DbaXMetadataType.Routine => client.GetRoutines(ConnectionString, Schema),
            _ => throw new NotSupportedException($"Metadata type '{Type}' is not supported.")
        };
    }

    private object GetOracleMetadata()
    {
        using var client = new DBAClientX.Oracle();
        return Type switch
        {
            DbaXMetadataType.Database => client.GetDatabases(ConnectionString),
            DbaXMetadataType.Table => client.GetTables(ConnectionString, Schema, !ExcludeViews.IsPresent),
            DbaXMetadataType.Column => client.GetColumns(ConnectionString, Schema, Table),
            DbaXMetadataType.Index => client.GetIndexes(ConnectionString, Schema, Table),
            DbaXMetadataType.ForeignKey => client.GetForeignKeys(ConnectionString, Schema, Table),
            DbaXMetadataType.Routine => client.GetRoutines(ConnectionString, Schema),
            _ => throw new NotSupportedException($"Metadata type '{Type}' is not supported.")
        };
    }

    private object GetSQLiteMetadata()
    {
        using var client = new DBAClientX.SQLite();
        return Type switch
        {
            DbaXMetadataType.Database => client.GetDatabases(ConnectionString),
            DbaXMetadataType.Table => client.GetTables(ConnectionString, Schema, !ExcludeViews.IsPresent),
            DbaXMetadataType.Column => client.GetColumns(ConnectionString, Schema, Table),
            DbaXMetadataType.Index => client.GetIndexes(ConnectionString, Schema, Table),
            DbaXMetadataType.ForeignKey => client.GetForeignKeys(ConnectionString, Schema, Table),
            DbaXMetadataType.Routine => client.GetRoutines(ConnectionString, Schema),
            _ => throw new NotSupportedException($"Metadata type '{Type}' is not supported.")
        };
    }
}
