namespace FabricClientX;

/// <summary>Represents a caller-acquired Microsoft Entra access token without printing its value.</summary>
public sealed class FabricAccessToken
{
    private FabricAccessToken(string token, DateTimeOffset expiresOn)
    {
        Token = token;
        ExpiresOn = expiresOn;
    }

    internal string Token { get; }

    /// <summary>Gets the token expiry timestamp.</summary>
    public DateTimeOffset ExpiresOn { get; }

    /// <summary>Creates a validated access-token value.</summary>
    public static FabricAccessToken Create(string token, DateTimeOffset expiresOn)
    {
        if (string.IsNullOrWhiteSpace(token))
        {
            throw new ArgumentException("An access token is required.", nameof(token));
        }

        return new FabricAccessToken(token, expiresOn);
    }

    /// <inheritdoc />
    public override string ToString() => $"Fabric access token expiring at {ExpiresOn:O}";
}
