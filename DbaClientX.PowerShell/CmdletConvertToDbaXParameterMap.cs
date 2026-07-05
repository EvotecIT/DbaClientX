using System.Collections;
using DBAClientX.Mapping;

namespace DBAClientX.PowerShell;

/// <summary>Converts objects into provider parameter dictionaries using the DbaClientX parameter mapper.</summary>
/// <example>
/// <summary>Map object properties to SQL parameters.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>[pscustomobject]@{ UserName = 'Ada' } | ConvertTo-DbaXParameterMap -Map @{ UserName = '@UserName' }</code>
/// <para>Returns a dictionary containing the provider parameter name and value.</para>
/// </example>
[Cmdlet(VerbsData.ConvertTo, "DbaXParameterMap")]
[CmdletBinding()]
public sealed class CmdletConvertToDbaXParameterMap : PSCmdlet
{
    /// <summary>Input object whose properties should be mapped to provider parameters.</summary>
    [Parameter(Mandatory = true, ValueFromPipeline = true)]
    [AllowNull]
    public object? InputObject { get; set; }

    /// <summary>Logical property name to provider parameter name map.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNullOrEmpty]
    public Hashtable Map { get; set; } = new();

    /// <summary>Optional ambient values used when the input object does not contain a mapped property.</summary>
    [Parameter(Mandatory = false)]
    public Hashtable? Ambient { get; set; }

    /// <summary>Converts enum values to strings rather than their underlying numeric value.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter EnumsAsString { get; set; }

    /// <summary>Preserves DateTimeOffset values instead of converting them to UTC DateTime values.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter PreserveDateTimeOffset { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        var map = DbaXProviderHelpers.ToStringDictionary(Map)!;
        var ambient = DbaXProviderHelpers.ToObjectDictionary(Ambient);
        var options = new DbParameterMapperOptions
        {
            EnumsAsString = EnumsAsString.IsPresent,
            DateTimeOffsetAsUtcDateTime = !PreserveDateTimeOffset.IsPresent
        };

        var item = NormalizeInputObject(InputObject);
        var result = DbParameterMapper.MapItem(item, map, options, ambient);
        WriteObject(result);
    }

    private static object? NormalizeInputObject(object? input)
    {
        if (input is not PSObject psObject)
        {
            return input;
        }

        var values = psObject.Properties
            .Where(static property => property.IsGettable)
            .ToDictionary(static property => property.Name, static property => NormalizeInputObject(property.Value), StringComparer.OrdinalIgnoreCase);
        return values.Count > 0 ? values : psObject.BaseObject;
    }
}
