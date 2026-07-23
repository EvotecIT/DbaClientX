using FabricClientX;
using FabricClientX.PowerBI;

namespace DBAClientX.PowerShell;

/// <summary>Cancels an accepted Power BI semantic-model refresh after explicit confirmation.</summary>
/// <example>
/// <summary>Cancel a refresh returned by Invoke-DbaXPowerBIRefresh.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>$refresh | Stop-DbaXPowerBIRefresh -TokenProvider $powerBiProvider -Confirm</code>
/// <para>Cancellation is never performed by discovery or settlement commands.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Stop, "DbaXPowerBIRefresh", SupportsShouldProcess = true, ConfirmImpact = ConfirmImpact.High)]
[OutputType(typeof(FabricResponse))]
[CmdletBinding()]
public sealed class CmdletStopDbaXPowerBIRefresh : AsyncPSCmdlet
{
    /// <summary>Caller-owned token provider configured for the Power BI API scope.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public IFabricTokenProvider TokenProvider { get; set; } = null!;

    /// <summary>Accepted refresh returned by Invoke-DbaXPowerBIRefresh.</summary>
    [Parameter(Mandatory = true, ValueFromPipeline = true, Position = 0)]
    [ValidateNotNull]
    public PowerBiRefreshStartResult Refresh { get; set; } = null!;

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        if (!ShouldProcess(
                Refresh.RefreshId.ToString("D"),
                "Cancel the Power BI semantic-model refresh"))
        {
            return;
        }

        var response = await FabricPowerShellClients
            .CreatePowerBiClient(TokenProvider)
            .CancelRefreshAsync(Refresh, CancelToken)
            .ConfigureAwait(false);
        WriteObject(response);
    }
}
