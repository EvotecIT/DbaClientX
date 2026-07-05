using DBAClientX.DataMovement;

namespace DBAClientX.PowerShell;

/// <summary>Creates a DbaClientX table-copy definition.</summary>
/// <example>
/// <summary>Create a copy definition.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>New-DbaXTableCopyDefinition -SourceName dbo.Users -DestinationName archive.Users -OrderByColumns Id</code>
/// <para>Returns a typed copy definition that can be passed to DbaClientX data movement APIs.</para>
/// </example>
[Cmdlet(VerbsCommon.New, "DbaXTableCopyDefinition")]
[CmdletBinding()]
public sealed class CmdletNewDbaXTableCopyDefinition : PSCmdlet
{
    /// <summary>Source table or view name.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string SourceName { get; set; } = string.Empty;

    /// <summary>Destination table name.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public string DestinationName { get; set; } = string.Empty;

    /// <summary>Optional logical display name.</summary>
    [Parameter(Mandatory = false)]
    public string? LogicalName { get; set; }

    /// <summary>Optional source-side order columns for paged reads.</summary>
    [Parameter(Mandatory = false)]
    public string[]? OrderByColumns { get; set; }

    /// <summary>Optional source column to destination column mappings.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? ColumnMappings { get; set; }

    /// <summary>Optional columns to exclude from copy pages.</summary>
    [Parameter(Mandatory = false)]
    public string[]? ExcludedColumns { get; set; }

    /// <summary>Optional destination column type conversions.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? ColumnTypeConversions { get; set; }

    /// <summary>Optional source columns used for source-side deduplication.</summary>
    [Parameter(Mandatory = false)]
    public string[]? DeduplicateByColumns { get; set; }

    /// <summary>Optional source order columns used to choose rows during deduplication.</summary>
    [Parameter(Mandatory = false)]
    public string[]? DeduplicateOrderByColumns { get; set; }

    /// <summary>Uses case-insensitive source-side deduplication.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter DeduplicateCaseInsensitive { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        var sourceOptions = DeduplicateByColumns is { Length: > 0 }
            ? new DbaTableCopySourceOptions(DeduplicateByColumns, DeduplicateOrderByColumns, DeduplicateCaseInsensitive.IsPresent)
            : null;

        var definition = new DbaTableCopyDefinition(
            SourceName,
            DestinationName,
            OrderByColumns,
            LogicalName,
            DbaXProviderHelpers.ToStringDictionary(ColumnMappings),
            ExcludedColumns,
            DbaXProviderHelpers.ToColumnTypeDictionary(ColumnTypeConversions),
            sourceOptions);
        definition.Validate();
        WriteObject(definition);
    }
}
