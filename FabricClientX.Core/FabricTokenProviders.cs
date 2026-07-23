namespace FabricClientX;

/// <summary>Adapts a caller-owned token callback to <see cref="IFabricTokenProvider"/>.</summary>
public sealed class DelegateFabricTokenProvider : IFabricTokenProvider
{
    private readonly Func<CancellationToken, ValueTask<FabricAccessToken>> _callback;

    /// <summary>Creates a provider over a caller-owned token callback.</summary>
    public DelegateFabricTokenProvider(
        Func<CancellationToken, ValueTask<FabricAccessToken>> callback)
    {
        _callback = callback ?? throw new ArgumentNullException(nameof(callback));
    }

    /// <inheritdoc />
    public ValueTask<FabricAccessToken> GetTokenAsync(CancellationToken cancellationToken)
        => _callback(cancellationToken);
}

/// <summary>
/// Supplies a fixed caller-provided token for short operations and rejects it near expiry.
/// </summary>
public sealed class FixedFabricTokenProvider : IFabricTokenProvider
{
    private readonly FabricAccessToken _token;
    private readonly TimeSpan _minimumValidity;

    /// <summary>Creates a fixed provider. The token is never written to diagnostic output.</summary>
    public FixedFabricTokenProvider(
        string token,
        DateTimeOffset expiresOn,
        TimeSpan? minimumValidity = null)
    {
        _token = FabricAccessToken.Create(token, expiresOn);
        _minimumValidity = minimumValidity ?? TimeSpan.FromMinutes(1);
        if (_minimumValidity < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(minimumValidity));
        }
    }

    /// <inheritdoc />
    public ValueTask<FabricAccessToken> GetTokenAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_token.ExpiresOn <= DateTimeOffset.UtcNow + _minimumValidity)
        {
            throw new InvalidOperationException(
                "The fixed Fabric access token is expired or too close to expiry.");
        }

        return new ValueTask<FabricAccessToken>(_token);
    }
}
