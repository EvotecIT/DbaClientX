# FabricClientX.PowerBI

Typed discovery and refresh-settlement workflows for Power BI semantic models.

The package uses the caller-owned authentication and HTTP lifetime configured through
`FabricClientX.Core`. It does not implement legacy push datasets.

## Install

```bash
dotnet add package FabricClientX.PowerBI
```

## Quick start

```csharp
using FabricClientX;
using FabricClientX.PowerBI;

using var httpClient = new HttpClient();
IFabricTokenProvider tokenProvider = new FixedFabricTokenProvider(accessToken, expiresOn);
var options = new FabricClientOptions(httpClient, tokenProvider)
{
    BaseAddress = PowerBiClient.DefaultBaseAddress
};
var client = new PowerBiClient(new FabricHttpClient(options));

var models = await client.ListSemanticModelsAsync(workspaceId);
```

The token provider must acquire tokens for the Power BI API scope exposed by
`PowerBiClient.DefaultScope`. Refresh mutations are explicit, are not retried, and can be
settled or cancelled through their returned refresh identity.
