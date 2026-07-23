using FabricClientX;

namespace FabricClientX.PowerShell;

/// <summary>Gets items from a Microsoft Fabric workspace.</summary>
/// <example>
/// <summary>List semantic models in one workspace.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-FabricXItem -TokenProvider $provider -WorkspaceId $workspaceId -Type SemanticModel</code>
/// <para>Uses the Fabric Core Items API and follows all continuation pages.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "FabricXItem")]
[OutputType(typeof(FabricItem))]
[CmdletBinding()]
public sealed class CmdletGetFabricXItem : AsyncPSCmdlet
{
    /// <summary>Caller-owned token provider configured for the Fabric API scope.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public IFabricTokenProvider TokenProvider { get; set; } = null!;

    /// <summary>Workspace identifier.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public Guid WorkspaceId { get; set; }

    /// <summary>Optional Fabric item type, such as SemanticModel or Warehouse.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string? Type { get; set; }

    /// <summary>Optional non-zero W3C trace identifier used to correlate the request.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string? OperationId { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var result = await FabricPowerShellClients
            .CreateWorkspaceClient(TokenProvider)
            .ListItemsAsync(
                WorkspaceId,
                Type,
                OperationId,
                CancelToken)
            .ConfigureAwait(false);
        FabricPowerShellClients.WriteCorrelatedValues(this, result);
    }
}
