using FabricClientX;
using FabricClientX.PowerBI;

namespace DBAClientX.PowerShell;

/// <summary>Requests a Power BI semantic-model refresh and optionally waits for settlement.</summary>
/// <example>
/// <summary>Refresh a semantic model and wait for its terminal state.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Invoke-DbaXPowerBIRefresh -TokenProvider $powerBiProvider -WorkspaceId $workspaceId -SemanticModelId $modelId -Wait -TimeoutMinutes 30</code>
/// <para>Returns refresh identity, terminal status, and the stable OperationId.</para>
/// </example>
[Cmdlet(VerbsLifecycle.Invoke, "DbaXPowerBIRefresh", SupportsShouldProcess = true)]
[OutputType(typeof(PowerBiRefreshStartResult), typeof(PowerBiRefreshSettlement))]
[CmdletBinding()]
public sealed class CmdletInvokeDbaXPowerBIRefresh : AsyncPSCmdlet
{
    /// <summary>Caller-owned token provider configured for the Power BI API scope.</summary>
    [Parameter(Mandatory = true)]
    [ValidateNotNull]
    public IFabricTokenProvider TokenProvider { get; set; } = null!;

    /// <summary>Power BI workspace identifier.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public Guid WorkspaceId { get; set; }

    /// <summary>Semantic-model identifier.</summary>
    [Parameter(Mandatory = true, Position = 1)]
    public Guid SemanticModelId { get; set; }

    /// <summary>Waits until the refresh reaches a terminal state.</summary>
    [Parameter]
    public SwitchParameter Wait { get; set; }

    /// <summary>Maximum settlement wait in minutes.</summary>
    [Parameter]
    [ValidateRange(1, 1440)]
    public int TimeoutMinutes { get; set; } = 60;

    /// <summary>Polling interval in seconds.</summary>
    [Parameter]
    [ValidateRange(1, 3600)]
    public int PollIntervalSeconds { get; set; } = 10;

    /// <summary>Power BI notification option. Omit it for service-principal enhanced refreshes.</summary>
    [Parameter]
    [ValidateSet("NoNotification", "MailOnFailure", "MailOnCompletion")]
    public string? NotifyOption { get; set; } = "NoNotification";

    /// <summary>Optional Power BI service-side retry count.</summary>
    [Parameter]
    [ValidateRange(0, 10)]
    public int? RetryCount { get; set; }

    /// <summary>Optional non-zero W3C trace identifier used to correlate the request.</summary>
    [Parameter]
    [ValidateNotNullOrEmpty]
    public string? OperationId { get; set; }

    /// <inheritdoc />
    protected override async Task ProcessRecordAsync()
    {
        var target = $"{WorkspaceId:D}/{SemanticModelId:D}";
        if (!ShouldProcess(target, "Request a Power BI semantic-model refresh"))
        {
            return;
        }

        var client = FabricPowerShellClients.CreatePowerBiClient(TokenProvider);
        var start = await client.StartRefreshAsync(
            WorkspaceId,
            SemanticModelId,
            new PowerBiRefreshRequest
            {
                NotifyOption = NotifyOption,
                RetryCount = RetryCount
            },
            OperationId,
            CancelToken).ConfigureAwait(false);
        if (!Wait.IsPresent)
        {
            WriteObject(start);
            return;
        }

        var settlement = await client.WaitForRefreshAsync(
            start,
            TimeSpan.FromMinutes(TimeoutMinutes),
            TimeSpan.FromSeconds(PollIntervalSeconds),
            CancelToken).ConfigureAwait(false);
        WriteObject(settlement);
    }
}
