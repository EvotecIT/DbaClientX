namespace FabricClientX;

/// <summary>Provides typed Microsoft Fabric workspace and item discovery.</summary>
public sealed class FabricWorkspaceClient
{
    private readonly FabricHttpClient _transport;

    /// <summary>Creates a workspace client over a caller-configured transport.</summary>
    public FabricWorkspaceClient(FabricHttpClient transport)
    {
        _transport = transport ?? throw new ArgumentNullException(nameof(transport));
    }

    /// <summary>Lists every workspace the authenticated principal can access.</summary>
    public Task<FabricCollectionResult<FabricWorkspace>> ListWorkspacesAsync(
        IEnumerable<string>? roles = null,
        bool preferWorkspaceSpecificEndpoints = false,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var query = new List<string>();
        var normalizedRoles = roles?
            .Where(static role => !string.IsNullOrWhiteSpace(role))
            .Select(static role => role.Trim())
            .ToArray();
        if (normalizedRoles is { Length: > 0 })
        {
            query.Add($"roles={Uri.EscapeDataString(string.Join(",", normalizedRoles))}");
        }

        if (preferWorkspaceSpecificEndpoints)
        {
            query.Add("preferWorkspaceSpecificEndpoints=true");
        }

        var uri = query.Count == 0 ? "workspaces" : $"workspaces?{string.Join("&", query)}";
        return _transport.GetAllPagesResultAsync<FabricWorkspace>(
            uri,
            operationId,
            cancellationToken);
    }

    /// <summary>Lists every matching item in a workspace.</summary>
    public Task<FabricCollectionResult<FabricItem>> ListItemsAsync(
        Guid workspaceId,
        string? itemType = null,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        if (workspaceId == Guid.Empty)
        {
            throw new ArgumentException("A workspace identifier is required.", nameof(workspaceId));
        }

        var uri = $"workspaces/{workspaceId:D}/items";
        if (!string.IsNullOrWhiteSpace(itemType))
        {
            uri += $"?type={Uri.EscapeDataString(itemType!.Trim())}";
        }

        return _transport.GetAllPagesResultAsync<FabricItem>(
            uri,
            operationId,
            cancellationToken);
    }
}
