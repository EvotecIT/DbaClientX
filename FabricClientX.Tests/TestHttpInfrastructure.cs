using System.Net;
using System.Net.Http.Headers;
using System.Text;
using FabricClientX;

namespace FabricClientX.Tests;

internal sealed class QueueHttpMessageHandler : HttpMessageHandler
{
    private readonly Queue<Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>> _responses = new();

    public List<RequestSnapshot> Requests { get; } = new();

    public void Enqueue(HttpStatusCode statusCode, string? json = null, Action<HttpResponseMessage>? configure = null)
        => _responses.Enqueue((_, _) =>
        {
            var response = new HttpResponseMessage(statusCode);
            if (json != null)
            {
                response.Content = new StringContent(json, Encoding.UTF8, "application/json");
            }

            configure?.Invoke(response);
            return Task.FromResult(response);
        });

    public void Enqueue(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> response)
        => _responses.Enqueue(response);

    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var body = request.Content == null
            ? null
            : await request.Content.ReadAsStringAsync(cancellationToken);
        Requests.Add(new RequestSnapshot(
            request.Method,
            request.RequestUri ?? throw new InvalidOperationException("Request URI was missing."),
            request.Headers.Authorization,
            body));
        if (_responses.Count == 0)
        {
            throw new InvalidOperationException("No queued response is available.");
        }

        return await _responses.Dequeue()(request, cancellationToken);
    }
}

internal sealed record RequestSnapshot(
    HttpMethod Method,
    Uri Uri,
    AuthenticationHeaderValue? Authorization,
    string? Body);

internal sealed class UnreadableUnknownLengthContent : HttpContent
{
    protected override Task SerializeToStreamAsync(
        Stream stream,
        TransportContext? context)
        => throw new InvalidOperationException(
            "Bodyless success content should not be read.");

    protected override bool TryComputeLength(out long length)
    {
        length = 0;
        return false;
    }
}

internal sealed class StaticTokenProvider : IFabricTokenProvider
{
    public const string Token = "super-secret-test-token";

    public int Calls { get; private set; }

    public ValueTask<FabricAccessToken> GetTokenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Calls++;
        return new ValueTask<FabricAccessToken>(
            FabricAccessToken.Create(Token, DateTimeOffset.UtcNow.AddHours(1)));
    }
}

internal static class TestClients
{
    public static FabricHttpClient Create(
        QueueHttpMessageHandler handler,
        StaticTokenProvider? tokenProvider = null,
        Uri? baseAddress = null,
        Func<TimeSpan, CancellationToken, Task>? delayAsync = null,
        int maxRetries = 3)
    {
        var options = new FabricClientOptions(
            new HttpClient(handler, disposeHandler: false),
            tokenProvider ?? new StaticTokenProvider())
        {
            BaseAddress = baseAddress ?? FabricClientOptions.DefaultBaseAddress,
            MaxRetryAttempts = maxRetries
        };
        if (delayAsync != null)
        {
            options.DelayAsync = delayAsync;
        }

        return new FabricHttpClient(options);
    }
}
