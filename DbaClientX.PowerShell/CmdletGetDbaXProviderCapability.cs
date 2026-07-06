namespace DBAClientX.PowerShell;

/// <summary>Gets the capabilities exposed by DbaClientX providers.</summary>
/// <example>
/// <summary>List all provider capabilities.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-DbaXProviderCapability</code>
/// <para>Returns one object per supported provider.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "DbaXProviderCapability")]
[CmdletBinding()]
public sealed class CmdletGetDbaXProviderCapability : PSCmdlet
{
    /// <summary>Optional provider filter.</summary>
    [Parameter(Mandatory = false, Position = 0)]
    public DbaXProvider[]? Provider { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        var providers = Provider is { Length: > 0 }
            ? Provider
            : Enum.GetValues(typeof(DbaXProvider)).Cast<DbaXProvider>().ToArray();

        foreach (var provider in providers)
        {
            var capabilities = DbaXProviderHelpers.GetCapabilities(provider);
            WriteObject(new PSObject(new
            {
                Provider = provider,
                Alias = DbaXProviderHelpers.GetAlias(provider),
                Capabilities = capabilities,
                CapabilityNames = Enum.GetValues(typeof(DbaXProviderCapability))
                    .Cast<DbaXProviderCapability>()
                    .Where(capability => capability != DbaXProviderCapability.None && capabilities.HasFlag(capability))
                    .Select(static capability => capability.ToString())
                    .ToArray()
            }));
        }
    }
}
