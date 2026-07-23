using FabricClientX;
using FabricClientX.PowerBI;

namespace FabricClientX.PowerShell;

/// <summary>Gets Power BI semantic models in a workspace.</summary>
/// <example>
/// <summary>List refreshable semantic models.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-FabricXPowerBISemanticModel -TokenProvider $powerBiProvider -WorkspaceId $workspaceId | Where-Object IsRefreshable</code>
/// <para>Returns typed semantic models with a stable OperationId property.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "FabricXPowerBISemanticModel")]
[OutputType(typeof(PowerBiSemanticModel))]
[CmdletBinding()]
public sealed class CmdletGetFabricXPowerBISemanticModel : AsyncPSCmdlet
{
    /// <summary>Caller-owned token provider configured for the Power BI API scope.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public IFabricTokenProvider TokenProvider { get; set; } = null!;

    /// <summary>Power BI workspace identifier.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public Guid WorkspaceId { get; set; }

    /// <summary>Optional non-zero W3C trace identifier used to correlate the request.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string? OperationId { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var result = await FabricPowerShellClients
            .CreatePowerBiClient(TokenProvider)
            .ListSemanticModelsAsync(WorkspaceId, OperationId, CancelToken)
            .ConfigureAwait(false);
        FabricPowerShellClients.WriteCorrelatedValues(this, result);
    }
}
