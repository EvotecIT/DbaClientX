using System.Diagnostics;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using DBAClientX.Diagnostics;

namespace FabricClientX;

/// <summary>
/// Provides authenticated, throttling-aware access to Microsoft Fabric-compatible REST endpoints.
/// </summary>
public sealed class FabricHttpClient
{
    private const string RequestIdHeader = "x-ms-request-id";
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly FabricClientOptions _options;

    /// <summary>Creates a transport over caller-owned authentication and HTTP resources.</summary>
    public FabricHttpClient(FabricClientOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();
    }

    /// <summary>Gets and deserializes a service resource.</summary>
    public Task<FabricResponse<T>> GetAsync<T>(
        string requestUri,
        string? operationId = null,
        CancellationToken cancellationToken = default)
        => SendTypedAsync<T>(HttpMethod.Get, requestUri, null, operationId, cancellationToken);

    /// <summary>Posts a JSON request without expecting a response body.</summary>
    public async Task<FabricResponse> PostAsync<TRequest>(
        string requestUri,
        TRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var response = await SendTypedAsync<object>(
            HttpMethod.Post,
            requestUri,
            request,
            operationId,
            cancellationToken,
            deserializeResponseBody: false).ConfigureAwait(false);
        return new FabricResponse(
            response.StatusCode,
            response.OperationId,
            response.RequestId,
            response.Location,
            response.ServiceOperationId,
            response.RetryAfter);
    }

    /// <summary>Posts a JSON request and deserializes the response body.</summary>
    public Task<FabricResponse<TResponse>> PostAsync<TRequest, TResponse>(
        string requestUri,
        TRequest request,
        string? operationId = null,
        CancellationToken cancellationToken = default)
        => SendTypedAsync<TResponse>(
            HttpMethod.Post,
            requestUri,
            request,
            operationId,
            cancellationToken);

    /// <summary>Deletes a service resource.</summary>
    public async Task<FabricResponse> DeleteAsync(
        string requestUri,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var response = await SendTypedAsync<object>(
            HttpMethod.Delete,
            requestUri,
            null,
            operationId,
            cancellationToken,
            deserializeResponseBody: false).ConfigureAwait(false);
        return new FabricResponse(
            response.StatusCode,
            response.OperationId,
            response.RequestId,
            response.Location,
            response.ServiceOperationId,
            response.RetryAfter);
    }

    /// <summary>Reads every page, following only service continuations on the configured host.</summary>
    public async Task<IReadOnlyList<T>> GetAllPagesAsync<T>(
        string requestUri,
        string? operationId = null,
        CancellationToken cancellationToken = default)
        => (await GetAllPagesResultAsync<T>(
            requestUri,
            operationId,
            cancellationToken).ConfigureAwait(false)).Values;

    /// <summary>
    /// Reads every page and returns the values with the stable cross-page operation identifier.
    /// </summary>
    public async Task<FabricCollectionResult<T>> GetAllPagesResultAsync<T>(
        string requestUri,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var values = new List<T>();
        var observedPages = new HashSet<string>(StringComparer.Ordinal);
        string? next = requestUri;
        string? stableOperationId = operationId;
        var pageCount = 0;
        while (!string.IsNullOrWhiteSpace(next))
        {
            var canonicalPage = ResolveRequestUri(next!).AbsoluteUri;
            if (!observedPages.Add(canonicalPage))
            {
                throw new InvalidOperationException(
                    "The service returned a repeated pagination continuation.");
            }

            pageCount++;
            if (pageCount > _options.MaxPaginationPages)
            {
                throw new InvalidOperationException(
                    "The service exceeded the configured pagination limit.");
            }

            var response = await GetAsync<FabricPage<T>>(
                next!,
                stableOperationId,
                cancellationToken).ConfigureAwait(false);
            stableOperationId = response.OperationId;
            if (response.Value?.Value != null)
            {
                values.AddRange(response.Value.Value);
            }

            next = response.Value?.ContinuationUri;
        }

        return new FabricCollectionResult<T>(
            values,
            stableOperationId ?? throw new InvalidOperationException(
                "The paged request did not establish an operation identifier."));
    }

    private async Task<FabricResponse<T>> SendTypedAsync<T>(
        HttpMethod method,
        string requestUri,
        object? requestBody,
        string? operationId,
        CancellationToken cancellationToken,
        bool deserializeResponseBody = true)
    {
        _options.Validate();
        var resolvedUri = ResolveRequestUri(requestUri);
        using var operation = DbaClientXDiagnostics.StartOperation(
            "FabricClientX.Http",
            operationId,
            new[]
            {
                new KeyValuePair<string, object?>("http.request.method", method.Method),
                new KeyValuePair<string, object?>("server.address", resolvedUri.Host)
            });

        var attempt = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempt++;
            using var request = await CreateRequestAsync(
                method,
                resolvedUri,
                requestBody,
                cancellationToken).ConfigureAwait(false);
            HttpResponseMessage response;
            try
            {
                response = await _options.HttpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (HttpRequestException ex) when (
                CanRetry(method) &&
                attempt <= _options.MaxRetryAttempts)
            {
                var delay = GetRetryDelay(attempt);
                DbaClientXDiagnostics.RecordRetry(attempt, delay, ex);
                await _options.DelayAsync(delay, cancellationToken).ConfigureAwait(false);
                continue;
            }
            catch (HttpRequestException ex)
            {
                DbaClientXDiagnostics.RecordException(operation.Activity, ex);
                throw new HttpRequestException(
                    "The Fabric request failed at the transport layer.");
            }

            using (response)
            {
                operation.Activity?.SetTag("http.response.status_code", (int)response.StatusCode);
                if (response.IsSuccessStatusCode)
                {
                    var value = deserializeResponseBody
                        ? await DeserializeAsync<T>(
                            response,
                            cancellationToken).ConfigureAwait(false)
                        : default;
                    return new FabricResponse<T>(
                        value,
                        response.StatusCode,
                        operation.OperationId,
                        GetRequestId(response),
                        response.Headers.Location,
                        GetHeader(response, "x-ms-operation-id"),
                        GetRetryAfter(response));
                }

                if (CanRetry(method, response.StatusCode) && attempt <= _options.MaxRetryAttempts)
                {
                    var delay = GetRetryDelay(response, attempt);
                    DbaClientXDiagnostics.RecordRetry(
                        attempt,
                        delay,
                        new HttpRequestException($"HTTP {(int)response.StatusCode}"));
                    await _options.DelayAsync(delay, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                var exception = await CreateExceptionAsync(response, cancellationToken).ConfigureAwait(false);
                DbaClientXDiagnostics.RecordException(operation.Activity, exception);
                throw exception;
            }
        }
    }

    private async Task<HttpRequestMessage> CreateRequestAsync(
        HttpMethod method,
        Uri requestUri,
        object? body,
        CancellationToken cancellationToken)
    {
        var token = await _options.TokenProvider.GetTokenAsync(cancellationToken).ConfigureAwait(false);
        if (token == null || string.IsNullOrWhiteSpace(token.Token))
        {
            throw new InvalidOperationException("The token provider returned an empty access token.");
        }

        var request = new HttpRequestMessage(method, requestUri);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        if (body != null)
        {
            var json = JsonSerializer.Serialize(body, body.GetType(), SerializerOptions);
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }

        return request;
    }

    private Uri ResolveRequestUri(string requestUri)
    {
        if (string.IsNullOrWhiteSpace(requestUri))
        {
            throw new ArgumentException("A request URI is required.", nameof(requestUri));
        }

        var resolved = Uri.TryCreate(requestUri, UriKind.Absolute, out var absolute)
            ? absolute
            : new Uri(_options.BaseAddress, requestUri);
        if (resolved.Scheme != Uri.UriSchemeHttps ||
            !string.Equals(
                resolved.Authority,
                _options.BaseAddress.Authority,
                StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "Request and continuation URIs must use HTTPS on the configured service authority.");
        }

        return resolved;
    }

    private static bool CanRetry(HttpMethod method, HttpStatusCode statusCode)
    {
        var retryable = statusCode == HttpStatusCode.RequestTimeout ||
                        statusCode == (HttpStatusCode)429 ||
                        (int)statusCode >= 500;
        return CanRetry(method) && retryable;
    }

    private static bool CanRetry(HttpMethod method)
        => method == HttpMethod.Get ||
           method == HttpMethod.Head ||
           method == HttpMethod.Options ||
           method == HttpMethod.Delete;

    private TimeSpan GetRetryDelay(HttpResponseMessage response, int attempt)
    {
        var retryAfter = response.Headers.RetryAfter;
        if (retryAfter?.Delta is { } delta)
        {
            return BoundDelay(delta);
        }

        if (retryAfter?.Date is { } date)
        {
            return BoundDelay(date - DateTimeOffset.UtcNow);
        }

        var multiplier = Math.Pow(2, Math.Max(0, attempt - 1));
        return BoundDelay(TimeSpan.FromMilliseconds(
            _options.MinimumRetryDelay.TotalMilliseconds * multiplier));
    }

    private TimeSpan GetRetryDelay(int attempt)
    {
        var multiplier = Math.Pow(2, Math.Max(0, attempt - 1));
        return BoundDelay(TimeSpan.FromMilliseconds(
            _options.MinimumRetryDelay.TotalMilliseconds * multiplier));
    }

    private TimeSpan BoundDelay(TimeSpan delay)
    {
        if (delay < _options.MinimumRetryDelay)
        {
            return _options.MinimumRetryDelay;
        }

        return delay > _options.MaximumRetryDelay ? _options.MaximumRetryDelay : delay;
    }

    private static async Task<T?> DeserializeAsync<T>(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        if (response.Content == null || response.Content.Headers.ContentLength == 0)
        {
            return default;
        }

        using var content = await ReadContentStreamAsync(
            response.Content,
            cancellationToken).ConfigureAwait(false);
        if (content.CanSeek && content.Length == 0)
        {
            return default;
        }

        return await JsonSerializer.DeserializeAsync<T>(
            content,
            SerializerOptions,
            cancellationToken).ConfigureAwait(false);
    }

    private static async Task<FabricApiException> CreateExceptionAsync(
        HttpResponseMessage response,
        CancellationToken cancellationToken)
    {
        string? code = null;
        if (response.Content != null)
        {
            try
            {
                using var content = await ReadContentStreamAsync(
                    response.Content,
                    cancellationToken).ConfigureAwait(false);
                using var document = await JsonDocument.ParseAsync(
                    content,
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                var error = document.RootElement.TryGetProperty("error", out var nested)
                    ? nested
                    : document.RootElement;
                code = TryGetString(error, "errorCode") ?? TryGetString(error, "code");
            }
            catch (JsonException)
            {
                // Raw response content is intentionally not retained.
            }
        }

        return new FabricApiException(
            response.StatusCode,
            code,
            GetRequestId(response));
    }

    private static string? TryGetString(JsonElement element, string propertyName)
        => element.ValueKind == JsonValueKind.Object &&
           element.TryGetProperty(propertyName, out var value) &&
           value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    private static Task<Stream> ReadContentStreamAsync(
        HttpContent content,
        CancellationToken cancellationToken)
    {
#if NET472
        cancellationToken.ThrowIfCancellationRequested();
        return content.ReadAsStreamAsync();
#else
        return content.ReadAsStreamAsync(cancellationToken);
#endif
    }

    private static string? GetRequestId(HttpResponseMessage response)
        => GetHeader(response, RequestIdHeader);

    private static string? GetHeader(HttpResponseMessage response, string name)
        => response.Headers.TryGetValues(name, out var values)
            ? values.FirstOrDefault()
            : null;

    private static TimeSpan? GetRetryAfter(HttpResponseMessage response)
    {
        if (response.Headers.RetryAfter?.Delta is { } delta)
        {
            return delta;
        }

        return response.Headers.RetryAfter?.Date is { } date
            ? date - DateTimeOffset.UtcNow
            : null;
    }

}
