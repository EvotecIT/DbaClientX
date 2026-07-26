using System.Net;
using FabricClientX.PowerBI;

namespace FabricClientX.Tests;

public sealed class PowerBiClientTests
{
    [Fact]
    public async Task RefreshAndWait_UsesAcceptedLocationAndStableOperationId()
    {
        var workspaceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var modelId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var refreshId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        const string operationId = "0123456789abcdef0123456789abcdef";
        var refreshLocation =
            $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId:D}/datasets/{modelId:D}/refreshes/{refreshId:D}";
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.Accepted,
            configure: response =>
            {
                response.Headers.Add("x-ms-request-id", refreshId.ToString("D"));
                response.Headers.Location = new Uri(refreshLocation);
            });
        handler.Enqueue(HttpStatusCode.Accepted, """{"status":"Unknown","numberOfAttempts":0}""");
        handler.Enqueue(
            HttpStatusCode.OK,
            """{"status":"Completed","extendedStatus":"Completed","numberOfAttempts":1}""");
        var transport = TestClients.Create(
            handler,
            baseAddress: PowerBiClient.DefaultBaseAddress);
        var client = new PowerBiClient(transport, static (_, _) => Task.CompletedTask);

        var settlement = await client.RefreshAndWaitAsync(
            workspaceId,
            modelId,
            new PowerBiRefreshRequest(),
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero,
            operationId);

        Assert.True(settlement.Succeeded);
        Assert.Equal(operationId, settlement.OperationId);
        Assert.Equal(refreshId, settlement.Start.RefreshId);
        Assert.Equal(HttpMethod.Post, handler.Requests[0].Method);
        Assert.DoesNotContain("\"notifyOption\"", handler.Requests[0].Body);
        Assert.Equal(new Uri(refreshLocation), handler.Requests[1].Uri);
        Assert.Equal(new Uri(refreshLocation), handler.Requests[2].Uri);
    }

    [Fact]
    public async Task ListSemanticModels_ReturnsTypedValues()
    {
        var workspaceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var modelId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.OK,
            $$"""{"value":[{"id":"{{modelId:D}}","name":"Executive","isRefreshable":true}]}""");
        var client = new PowerBiClient(TestClients.Create(
            handler,
            baseAddress: PowerBiClient.DefaultBaseAddress));

        var models = await client.ListSemanticModelsAsync(workspaceId);

        Assert.Equal(modelId, models.Values.Single().Id);
        Assert.True(models.Values.Single().IsRefreshable);
        Assert.EndsWith($"/groups/{workspaceId:D}/datasets", handler.Requests.Single().Uri.AbsoluteUri);
    }

    [Fact]
    public async Task WaitForRefresh_RejectsUnsupportedStatusInsteadOfReportingSettlement()
    {
        var workspaceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var modelId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var refreshId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        var location = new Uri(
            $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId:D}/datasets/{modelId:D}/refreshes/{refreshId:D}");
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.Accepted,
            configure: response =>
            {
                response.Headers.Add("x-ms-request-id", refreshId.ToString("D"));
                response.Headers.Location = location;
            });
        handler.Enqueue(HttpStatusCode.OK, """{"status":"FutureStatus","numberOfAttempts":1}""");
        var client = new PowerBiClient(
            TestClients.Create(handler, baseAddress: PowerBiClient.DefaultBaseAddress),
            static (_, _) => Task.CompletedTask);
        var start = await client.StartRefreshAsync(
            workspaceId,
            modelId,
            request: null,
            operationId: "0123456789abcdef0123456789abcdef");

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.WaitForRefreshAsync(start, TimeSpan.FromMinutes(1), TimeSpan.Zero));

        Assert.Contains("unsupported refresh status", exception.Message);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task WaitForRefresh_ReturnsTimedOutAsTerminalSettlement()
    {
        var workspaceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var modelId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var refreshId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        var location = new Uri(
            $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId:D}/datasets/{modelId:D}/refreshes/{refreshId:D}");
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.Accepted,
            configure: response =>
            {
                response.Headers.Add("x-ms-request-id", refreshId.ToString("D"));
                response.Headers.Location = location;
            });
        handler.Enqueue(
            HttpStatusCode.OK,
            """{"status":"TimedOut","extendedStatus":"TimedOut","numberOfAttempts":1}""");
        var client = new PowerBiClient(
            TestClients.Create(handler, baseAddress: PowerBiClient.DefaultBaseAddress),
            static (_, _) => Task.CompletedTask);
        var start = await client.StartRefreshAsync(workspaceId, modelId);

        var settlement = await client.WaitForRefreshAsync(
            start,
            TimeSpan.FromMinutes(1),
            TimeSpan.Zero);

        Assert.False(settlement.Succeeded);
        Assert.Equal("TimedOut", settlement.Detail.Status);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task WaitForRefresh_BoundsAnInFlightStatusRequestByTimeout()
    {
        var workspaceId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        var modelId = Guid.Parse("22222222-2222-2222-2222-222222222222");
        var refreshId = Guid.Parse("33333333-3333-3333-3333-333333333333");
        var location = new Uri(
            $"https://api.powerbi.com/v1.0/myorg/groups/{workspaceId:D}/datasets/{modelId:D}/refreshes/{refreshId:D}");
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.Accepted,
            configure: response =>
            {
                response.Headers.Add("x-ms-request-id", refreshId.ToString("D"));
                response.Headers.Location = location;
            });
        handler.Enqueue(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        var client = new PowerBiClient(
            TestClients.Create(handler, baseAddress: PowerBiClient.DefaultBaseAddress));
        var start = await client.StartRefreshAsync(workspaceId, modelId);

        await Assert.ThrowsAsync<TimeoutException>(() =>
            client.WaitForRefreshAsync(
                start,
                TimeSpan.FromMilliseconds(50),
                TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task StartRefresh_RejectsNotificationForEnhancedRequest()
    {
        var handler = new QueueHttpMessageHandler();
        var client = new PowerBiClient(TestClients.Create(
            handler,
            baseAddress: PowerBiClient.DefaultBaseAddress));

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            client.StartRefreshAsync(
                Guid.Parse("11111111-1111-1111-1111-111111111111"),
                Guid.Parse("22222222-2222-2222-2222-222222222222"),
                new PowerBiRefreshRequest
                {
                    NotifyOption = "NoNotification",
                    RetryCount = 1
                }));

        Assert.Contains("must be omitted", exception.Message);
        Assert.Empty(handler.Requests);
    }
}
