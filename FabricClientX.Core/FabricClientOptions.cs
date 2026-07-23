using System.Net.Http;

namespace FabricClientX;

/// <summary>Configures a <see cref="FabricHttpClient"/> without transferring resource ownership.</summary>
public sealed class FabricClientOptions
{
    /// <summary>The default Microsoft Fabric REST endpoint.</summary>
    public static readonly Uri DefaultBaseAddress = new("https://api.fabric.microsoft.com/v1/");

    /// <summary>Creates client options with caller-owned authentication and HTTP lifetime.</summary>
    public FabricClientOptions(HttpClient httpClient, IFabricTokenProvider tokenProvider)
    {
        HttpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        TokenProvider = tokenProvider ?? throw new ArgumentNullException(nameof(tokenProvider));
    }

    /// <summary>Gets the caller-owned HTTP client. FabricClientX never disposes it.</summary>
    public HttpClient HttpClient { get; }

    /// <summary>Gets the caller-owned token provider.</summary>
    public IFabricTokenProvider TokenProvider { get; }

    /// <summary>Gets or sets the service base address.</summary>
    public Uri BaseAddress { get; set; } = DefaultBaseAddress;

    /// <summary>Gets or sets the number of retries after the initial idempotent request.</summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>Gets or sets the maximum number of pages followed by one collection request.</summary>
    public int MaxPaginationPages { get; set; } = 10_000;

    /// <summary>Gets or sets the minimum exponential retry delay.</summary>
    public TimeSpan MinimumRetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Gets or sets the maximum retry delay.</summary>
    public TimeSpan MaximumRetryDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets the asynchronous delay implementation. This is primarily useful for
    /// deterministic host testing and remains caller-owned.
    /// </summary>
    public Func<TimeSpan, CancellationToken, Task> DelayAsync { get; set; } =
        static (delay, cancellationToken) => Task.Delay(delay, cancellationToken);

    internal void Validate()
    {
        if (BaseAddress == null || !BaseAddress.IsAbsoluteUri || BaseAddress.Scheme != Uri.UriSchemeHttps)
        {
            throw new ArgumentException("BaseAddress must be an absolute HTTPS URI.", nameof(BaseAddress));
        }

        if (!BaseAddress.AbsolutePath.EndsWith("/", StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "BaseAddress must end with '/' so relative Fabric resource paths preserve the configured base path.",
                nameof(BaseAddress));
        }

        if (MaxRetryAttempts < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxRetryAttempts));
        }

        if (MaxPaginationPages <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxPaginationPages));
        }

        if (MinimumRetryDelay < TimeSpan.Zero || MaximumRetryDelay < MinimumRetryDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(MinimumRetryDelay));
        }

        if (DelayAsync == null)
        {
            throw new ArgumentNullException(nameof(DelayAsync));
        }
    }
}
