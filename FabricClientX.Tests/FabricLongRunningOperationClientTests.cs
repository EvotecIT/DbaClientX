using System.Net;
using FabricClientX;

namespace FabricClientX.Tests;

public sealed class FabricLongRunningOperationClientTests
{
    [Fact]
    public async Task WaitForCompletion_PollsStateAndRetrievesResult()
    {
        var serviceOperationId = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.OK,
            """{"status":"Running","percentComplete":25}""",
            response => response.Headers.RetryAfter =
                new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(3)));
        handler.Enqueue(
            HttpStatusCode.OK,
            """{"status":"Succeeded","percentComplete":100}""",
            response => response.Headers.Location =
                new Uri($"https://api.fabric.microsoft.com/v1/operations/{serviceOperationId:D}/result"));
        handler.Enqueue(HttpStatusCode.OK, """{"id":"result-1"}""");
        var delays = new List<TimeSpan>();
        var transport = TestClients.Create(handler);
        var client = new FabricLongRunningOperationClient(
            transport,
            (delay, _) =>
            {
                delays.Add(delay);
                return Task.CompletedTask;
            });

        var result = await client.WaitForCompletionAsync<OperationPayload>(
            serviceOperationId,
            TimeSpan.FromMinutes(1));

        Assert.Equal("result-1", result.Value?.Id);
        Assert.Equal("Succeeded", result.State.Status);
        Assert.Equal(new[] { TimeSpan.FromSeconds(3) }, delays);
        Assert.Equal(3, handler.Requests.Count);
    }

    private sealed class OperationPayload
    {
        public string? Id { get; set; }
    }
}
