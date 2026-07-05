using DBAClientX.DataMovement;
using DBAClientX.Metadata;

namespace DBAClientX.PowerShell;

/// <summary>Discovers provider metadata and builds a DbaClientX table-copy plan.</summary>
/// <example>
/// <summary>Build a SQL Server copy plan from live metadata.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXTableCopyPlan -Provider SqlServer -ConnectionString $source -DestinationConnectionString $destination -DestinationSchema archive</code>
/// <para>Discovers source and destination metadata, then calls the shared DbaClientX planner.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXTableCopyPlan")]
[CmdletBinding()]
public sealed class CmdletGetDbaXTableCopyPlan : PSCmdlet
{
    /// <summary>Provider used to discover metadata.</summary>
    [Parameter(Mandatory = true)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Source provider connection string, or SQLite source database path.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Optional destination provider connection string, or SQLite destination database path.</summary>
    [Parameter(Mandatory = false)]
    public string? DestinationConnectionString { get; set; }

    /// <summary>Restricts source metadata to a schema.</summary>
    [Parameter(Mandatory = false)]
    public string? SourceSchema { get; set; }

    /// <summary>Restricts source metadata to a table.</summary>
    [Parameter(Mandatory = false)]
    public string? SourceTable { get; set; }

    /// <summary>Destination schema used for generated destination names.</summary>
    [Parameter(Mandatory = false)]
    public string? DestinationSchema { get; set; }

    /// <summary>Includes views in generated copy definitions.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeViews { get; set; }

    /// <summary>Optional source table to destination table mappings.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? TableMappings { get; set; }

    /// <summary>Optional global source column to destination column mappings.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? ColumnMappings { get; set; }

    /// <summary>Optional global excluded columns.</summary>
    [Parameter(Mandatory = false)]
    public string[]? ExcludedColumns { get; set; }

    /// <summary>Optional global column type conversions.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? ColumnTypeConversions { get; set; }

    /// <summary>Includes destination identity columns in generated pages.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeDestinationIdentityColumns { get; set; }

    /// <summary>Includes source generated columns in generated pages.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeSourceGeneratedColumns { get; set; }

    /// <summary>Includes destination generated columns in generated pages.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter IncludeDestinationGeneratedColumns { get; set; }

    /// <summary>Does not require source columns to exist in destination metadata.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter SkipDestinationColumnMatch { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        var tableMappings = DbaXProviderHelpers.ToStringDictionary(TableMappings);
        var sourceTables = FilterSourceTables(
            DbaXProviderHelpers.GetTables(Provider, ConnectionString, SourceSchema, IncludeViews.IsPresent),
            SourceSchema,
            SourceTable);
        var sourceColumns = DbaXProviderHelpers.GetColumns(Provider, ConnectionString, SourceSchema, SourceTable);
        var sourceIndexes = DbaXProviderHelpers.GetIndexes(Provider, ConnectionString, SourceSchema, SourceTable);
        var destinationColumns = string.IsNullOrWhiteSpace(DestinationConnectionString)
            ? null
            : DbaXProviderHelpers.GetColumns(Provider, DestinationConnectionString!, GetDestinationColumnSchemaFilter(DestinationSchema, tableMappings), null);

        var options = new DbaTableCopyPlanOptions
        {
            IdentifierProvider = DbaXProviderHelpers.ToTableCopyProvider(Provider),
            SourceSchema = SourceSchema,
            DestinationSchema = DestinationSchema,
            IncludeViews = IncludeViews.IsPresent,
            TableMappings = tableMappings,
            ColumnMappings = DbaXProviderHelpers.ToStringDictionary(ColumnMappings),
            ExcludedColumns = ExcludedColumns,
            ColumnTypeConversions = DbaXProviderHelpers.ToColumnTypeDictionary(ColumnTypeConversions),
            ExcludeDestinationIdentityColumns = !IncludeDestinationIdentityColumns.IsPresent,
            ExcludeSourceGeneratedColumns = !IncludeSourceGeneratedColumns.IsPresent,
            ExcludeDestinationGeneratedColumns = !IncludeDestinationGeneratedColumns.IsPresent,
            MatchDestinationColumns = !SkipDestinationColumnMatch.IsPresent
        };

        WriteObject(DbaTableCopyPlanner.BuildPlan(sourceTables, sourceColumns, sourceIndexes, destinationColumns, options));
    }

    internal static IReadOnlyList<DbaTableInfo> FilterSourceTables(IEnumerable<DbaTableInfo> sourceTables, string? sourceSchema, string? sourceTable)
    {
        if (string.IsNullOrWhiteSpace(sourceTable))
        {
            return sourceTables.ToArray();
        }

        return sourceTables
            .Where(table =>
                string.Equals(table.Name, sourceTable, StringComparison.OrdinalIgnoreCase) &&
                (string.IsNullOrWhiteSpace(sourceSchema) || string.Equals(table.Schema, sourceSchema, StringComparison.OrdinalIgnoreCase)))
            .ToArray();
    }

    internal static string? GetDestinationColumnSchemaFilter(string? destinationSchema, IReadOnlyDictionary<string, string>? tableMappings)
        => tableMappings is { Count: > 0 }
            ? null
            : destinationSchema;
}
