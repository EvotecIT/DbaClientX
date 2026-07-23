using FabricClientX;

namespace FabricClientX.PowerShell;

/// <summary>Gets Microsoft Fabric workspaces visible to the authenticated principal.</summary>
/// <example>
/// <summary>List accessible workspaces.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Get-FabricXWorkspace -TokenProvider $provider</code>
/// <para>Returns workspace objects with a stable OperationId property.</para>
/// </example>
[Cmdlet(VerbsCommon.Get, "FabricXWorkspace")]
[OutputType(typeof(FabricWorkspace))]
[CmdletBinding()]
public sealed class CmdletGetFabricXWorkspace : AsyncPSCmdlet
{
    /// <summary>Caller-owned token provider configured for the Fabric API scope.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public IFabricTokenProvider TokenProvider { get; set; } = null!;

    /// <summary>Optional workspace roles such as Admin, Member, Contributor, or Viewer.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string[]? Role { get; set; }

    /// <summary>Includes workspace-specific endpoints when the service supports them.</summary>
    [Parameter]
    public SwitchParameter PreferWorkspaceSpecificEndpoint { get; set; }

    /// <summary>Optional non-zero W3C trace identifier used to correlate the request.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string? OperationId { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var result = await FabricPowerShellClients
            .CreateWorkspaceClient(TokenProvider)
            .ListWorkspacesAsync(
                Role,
                PreferWorkspaceSpecificEndpoint.IsPresent,
                OperationId,
                CancelToken)
            .ConfigureAwait(false);
        FabricPowerShellClients.WriteCorrelatedValues(this, result);
    }
}
