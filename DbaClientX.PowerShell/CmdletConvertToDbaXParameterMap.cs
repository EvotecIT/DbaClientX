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

        var item = NormalizeInputObject(InputObject, map.Keys);
        var result = DbParameterMapper.MapItem(item, map, options, ambient);
        WriteObject(result);
    }

    private static object? NormalizeInputObject(object? input, IEnumerable<string>? mappedPaths = null)
    {
        if (input is not PSObject psObject)
        {
            return input;
        }

        var requestedPaths = mappedPaths?
            .Where(static path => !string.IsNullOrWhiteSpace(path))
            .Select(static path => path.Split(new[] { '.' }, 2))
            .GroupBy(static parts => parts[0], StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                static group => group.Key,
                static group => group
                    .Where(static parts => parts.Length > 1 && !string.IsNullOrWhiteSpace(parts[1]))
                    .Select(static parts => parts[1])
                    .ToArray(),
                StringComparer.OrdinalIgnoreCase);

        if (requestedPaths is not { Count: > 0 })
        {
            return psObject.BaseObject;
        }

        var values = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var requested in requestedPaths)
        {
            var property = psObject.Properties
                .FirstOrDefault(candidate => candidate.IsGettable && string.Equals(candidate.Name, requested.Key, StringComparison.OrdinalIgnoreCase));
            if (property == null)
            {
                continue;
            }

            values[property.Name] = NormalizeInputObject(property.Value, requested.Value);
        }

        return values;
    }
}
