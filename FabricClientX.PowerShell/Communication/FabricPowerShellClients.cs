using System.Net.Http;
using FabricClientX;
using FabricClientX.PowerBI;

namespace FabricClientX.PowerShell;

internal static class FabricPowerShellClients
{
    private static readonly HttpClient FabricHttpClient = new();
    private static readonly HttpClient PowerBiHttpClient = new();

    public static FabricWorkspaceClient CreateWorkspaceClient(IFabricTokenProvider tokenProvider)
        => new(CreateTransport(
            FabricHttpClient,
            tokenProvider,
            FabricClientOptions.DefaultBaseAddress));

    public static PowerBiClient CreatePowerBiClient(IFabricTokenProvider tokenProvider)
        => new(CreateTransport(
            PowerBiHttpClient,
            tokenProvider,
            PowerBiClient.DefaultBaseAddress));

    public static void WriteCorrelatedValues<T>(
        AsyncPSCmdlet cmdlet,
        FabricCollectionResult<T> result)
    {
        foreach (var value in result.Values)
        {
            var output = PSObject.AsPSObject(value);
            output.Properties.Add(new PSNoteProperty("OperationId", result.OperationId));
            cmdlet.WriteObject(output);
        }
    }

    private static FabricHttpClient CreateTransport(
        HttpClient httpClient,
        IFabricTokenProvider tokenProvider,
        Uri baseAddress)
        => new(new FabricClientOptions(httpClient, tokenProvider)
        {
            BaseAddress = baseAddress
        });
}
