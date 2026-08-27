# FabricClientX.Core

Typed, caller-controlled transport primitives for Microsoft Fabric REST APIs.

Authentication and `HttpClient` lifetime remain caller-owned. The library adds safe error
normalization, throttling-aware retries for idempotent requests, pagination, diagnostics,
and W3C operation correlation. It does not acquire or persist credentials.

## Install

```bash
dotnet add package FabricClientX.Core
```

## Quick start

```csharp
using FabricClientX;

using var httpClient = new HttpClient();
IFabricTokenProvider tokenProvider = new FixedFabricTokenProvider(accessToken, expiresOn);
var transport = new FabricHttpClient(new FabricClientOptions(httpClient, tokenProvider));
var workspaces = new FabricWorkspaceClient(transport);

var result = await workspaces.ListWorkspacesAsync();
foreach (var workspace in result.Values)
{
    Console.WriteLine($"{workspace.DisplayName} ({workspace.Type})");
}
```

The token provider must acquire tokens for `https://api.fabric.microsoft.com`. Use
`DelegateFabricTokenProvider` when the caller needs renewable credentials for long-running
operations. The API is pre-1.0 and may evolve as more Fabric resource contracts are added.
