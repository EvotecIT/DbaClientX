using System.Net;
using System.Net.Http.Headers;
using FabricClientX;

namespace FabricClientX.Tests;

public sealed class FabricHttpClientTests
{
    [Fact]
    public async Task GetAllPages_HonorsRetryAfterAndPreservesCorrelation()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            (HttpStatusCode)429,
            """{"errorCode":"RequestBlocked","message":"wait"}""",
            response => response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromSeconds(2)));
        handler.Enqueue(
            HttpStatusCode.OK,
            """
            {
              "value": [{"id":"11111111-1111-1111-1111-111111111111","displayName":"One","type":"Workspace"}],
              "continuationUri":"https://api.fabric.microsoft.com/v1/workspaces?continuationToken=next"
            }
            """);
        handler.Enqueue(
            HttpStatusCode.OK,
            """
            {
              "value": [{"id":"22222222-2222-2222-2222-222222222222","displayName":"Two","type":"Workspace"}]
            }
            """);
        var delays = new List<TimeSpan>();
        var client = TestClients.Create(
            handler,
            delayAsync: (delay, _) =>
            {
                delays.Add(delay);
                return Task.CompletedTask;
            });

        var result = await client.GetAllPagesResultAsync<FabricWorkspace>("workspaces");

        Assert.Equal(2, result.Values.Count);
        Assert.Equal(32, result.OperationId.Length);
        Assert.Equal(new[] { TimeSpan.FromSeconds(2) }, delays);
        Assert.Equal(3, handler.Requests.Count);
        Assert.All(handler.Requests, request =>
        {
            Assert.Equal("Bearer", request.Authorization?.Scheme);
            Assert.Equal(StaticTokenProvider.Token, request.Authorization?.Parameter);
        });
    }

    [Fact]
    public async Task GetAllPages_RejectsContinuationOnAnotherHostBeforeSendingToken()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.OK,
            """
            {
              "value": [],
              "continuationUri":"https://attacker.invalid/collect"
            }
            """);
        var client = TestClients.Create(handler);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.GetAllPagesAsync<FabricWorkspace>("workspaces"));

        Assert.Contains("configured service host", exception.Message);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task GetAllPages_RejectsRepeatedContinuation()
    {
        const string continuation =
            "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=same";
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.OK,
            $$"""{"value":[],"continuationUri":"{{continuation}}"}""");
        handler.Enqueue(
            HttpStatusCode.OK,
            $$"""{"value":[],"continuationUri":"{{continuation}}"}""");
        var client = TestClients.Create(handler);

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            client.GetAllPagesAsync<FabricWorkspace>("workspaces"));

        Assert.Contains("repeated pagination continuation", exception.Message);
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Post_DoesNotRetryNonIdempotentRequest()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue((HttpStatusCode)429, """{"errorCode":"RequestBlocked"}""");
        var client = TestClients.Create(handler);

        var exception = await Assert.ThrowsAsync<FabricApiException>(() =>
            client.PostAsync("workspaces", new { displayName = "Example" }));

        Assert.Equal((HttpStatusCode)429, exception.StatusCode);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task Get_RetriesTransientTransportFailureWithFreshRequest()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue((_, _) =>
            throw new HttpRequestException($"transport {StaticTokenProvider.Token}"));
        handler.Enqueue(HttpStatusCode.OK, """{"value":[]}""");
        var delays = new List<TimeSpan>();
        var client = TestClients.Create(
            handler,
            delayAsync: (delay, _) =>
            {
                delays.Add(delay);
                return Task.CompletedTask;
            });

        var response = await client.GetAsync<FabricPage<FabricWorkspace>>("workspaces");

        Assert.NotNull(response.Value);
        Assert.Equal(2, handler.Requests.Count);
        Assert.Single(delays);
    }

    [Fact]
    public async Task TransportFailure_DoesNotExposeUnderlyingMessage()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue((_, _) =>
            throw new HttpRequestException($"transport {StaticTokenProvider.Token}"));
        var client = TestClients.Create(handler, maxRetries: 0);

        var exception = await Assert.ThrowsAsync<HttpRequestException>(() =>
            client.GetAsync<object>("workspaces"));

        Assert.DoesNotContain(StaticTokenProvider.Token, exception.ToString());
        Assert.Null(exception.InnerException);
        Assert.Single(handler.Requests);
    }

    [Fact]
    public async Task Error_DoesNotExposeTokenOrRawServiceMessage()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(
            HttpStatusCode.BadRequest,
            $$"""{"errorCode":"BadInput","message":"leaked {{StaticTokenProvider.Token}}"}""",
            response => response.Headers.Add("x-ms-request-id", "request-123"));
        var client = TestClients.Create(handler);

        var exception = await Assert.ThrowsAsync<FabricApiException>(() =>
            client.GetAsync<object>("workspaces"));

        Assert.Equal("BadInput", exception.ErrorCode);
        Assert.Equal("request-123", exception.RequestId);
        Assert.DoesNotContain(StaticTokenProvider.Token, exception.ToString());
        Assert.DoesNotContain("leaked", exception.ToString());
    }

    [Fact]
    public async Task RequestCancellation_RemainsCancellation()
    {
        var handler = new QueueHttpMessageHandler();
        handler.Enqueue(async (_, cancellationToken) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        var client = TestClients.Create(handler);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            client.GetAsync<object>("workspaces", cancellationToken: cancellation.Token));
    }
}
