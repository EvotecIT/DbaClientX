using System.Net;
using FabricClientX;

namespace FabricClientX.Tests;

public sealed class FabricWorkspaceClientTests
{
    [Fact]
    public async Task Discovery_UsesTypedWorkspaceAndSemanticModelFilters()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.OK,
            """{"value":[{"id":"11111111-1111-1111-1111-111111111111","displayName":"Finance","type":"Workspace"}]}""");
        handler.Enqueue(
            HttpStatusCode.OK,
            """{"value":[{"id":"22222222-2222-2222-2222-222222222222","displayName":"Model","type":"SemanticModel","workspaceId":"11111111-1111-1111-1111-111111111111"}]}""");
        var client = new FabricWorkspaceClient(TestClients.Create(handler));

        var workspaces = await client.ListWorkspacesAsync(new[] { "Admin", "Member" });
        var items = await client.ListItemsAsync(
            workspaces.Values.Single().Id,
            "SemanticModel",
            workspaces.OperationId);

        Assert.Equal("Finance", workspaces.Values.Single().DisplayName);
        Assert.Equal("Model", items.Values.Single().DisplayName);
        Assert.Contains("roles=Admin%2CMember", handler.Requests[0].Uri.Query);
        Assert.Contains("type=SemanticModel", handler.Requests[1].Uri.Query);
        Assert.Equal(workspaces.OperationId, items.OperationId);
    }
}
