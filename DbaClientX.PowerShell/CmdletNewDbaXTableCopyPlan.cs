using DBAClientX.DataMovement;
using DBAClientX.Metadata;

namespace DBAClientX.PowerShell;

/// <summary>Builds a table-copy plan from supplied DbaClientX metadata objects.</summary>
/// <example>
/// <summary>Build a copy plan from metadata.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXTableCopyPlan -SourceTables $tables -SourceColumns $columns -Provider SqlServer -DestinationSchema archive</code>
/// <para>Calls the shared DbaClientX table-copy planner.</para>
/// </example>
[Cmdlet(VerbsCommon.New, "DbaXTableCopyPlan")]
[CmdletBinding()]
public sealed class CmdletNewDbaXTableCopyPlan : PSCmdlet
{
    /// <summary>Source table metadata.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public DbaTableInfo[] SourceTables { get; set; } = Array.Empty<DbaTableInfo>();

    /// <summary>Source column metadata.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public DbaColumnInfo[] SourceColumns { get; set; } = Array.Empty<DbaColumnInfo>();

    /// <summary>Optional source index metadata.</summary>
    [Parameter(Mandatory = false)]
    public DbaIndexInfo[]? SourceIndexes { get; set; }

    /// <summary>Optional destination column metadata.</summary>
    [Parameter(Mandatory = false)]
    public DbaColumnInfo[]? DestinationColumns { get; set; }

    /// <summary>Provider whose identifier folding rules should be used.</summary>
    [Parameter(Mandatory = false)]
    public DbaXProvider? Provider { get; set; }

    /// <summary>Restricts source tables to a schema.</summary>
    [Parameter(Mandatory = false)]
    public string? SourceSchema { get; set; }

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
        var options = new DbaTableCopyPlanOptions
        {
            IdentifierProvider = Provider.HasValue ? DbaXProviderHelpers.ToTableCopyProvider(Provider.Value) : null,
            SourceSchema = SourceSchema,
            DestinationSchema = DestinationSchema,
            IncludeViews = IncludeViews.IsPresent,
            TableMappings = DbaXProviderHelpers.ToStringDictionary(TableMappings),
            ColumnMappings = DbaXProviderHelpers.ToStringDictionary(ColumnMappings),
            ExcludedColumns = ExcludedColumns,
            ColumnTypeConversions = DbaXProviderHelpers.ToColumnTypeDictionary(ColumnTypeConversions),
            ExcludeDestinationIdentityColumns = !IncludeDestinationIdentityColumns.IsPresent,
            ExcludeSourceGeneratedColumns = !IncludeSourceGeneratedColumns.IsPresent,
            ExcludeDestinationGeneratedColumns = !IncludeDestinationGeneratedColumns.IsPresent,
            MatchDestinationColumns = !SkipDestinationColumnMatch.IsPresent
        };

        WriteObject(DbaTableCopyPlanner.BuildPlan(SourceTables, SourceColumns, SourceIndexes, DestinationColumns, options));
    }
}
