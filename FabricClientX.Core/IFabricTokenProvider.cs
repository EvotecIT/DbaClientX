namespace FabricClientX;

/// <summary>Supplies caller-owned Microsoft Entra access tokens.</summary>
public interface IFabricTokenProvider
{
    /// <summary>Gets a token appropriate for the configured Fabric or Power BI endpoint.</summary>
    ValueTask<FabricAccessToken> GetTokenAsync(CancellationToken cancellationToken);
}
