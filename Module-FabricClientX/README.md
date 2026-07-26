# FabricClientX PowerShell Module

FabricClientX provides PowerShell commands for Microsoft Fabric workspace discovery, Power BI semantic-model refreshes, and CSV ingestion into Fabric Warehouse.

The module is still unreleased. After publication is authorized, the intended installation path is:

```powershell
Install-Module FabricClientX -Scope CurrentUser
Import-Module FabricClientX
```

## Connect to the Fabric API

FabricClientX does not acquire or persist credentials. Pass a caller-acquired secure token and its expiry:

```powershell
$token = Get-AzAccessToken `
    -ResourceUrl 'https://api.fabric.microsoft.com' `
    -AsSecureString

$provider = New-FabricXTokenProvider `
    -AccessToken $token.Token `
    -ExpiresOn $token.ExpiresOn

$workspaces = Get-FabricXWorkspace -TokenProvider $provider
foreach ($workspace in $workspaces) {
    Get-FabricXItem -TokenProvider $provider -WorkspaceId $workspace.Id
}
```

Use a token issued for `https://analysis.windows.net/powerbi/api` with the Power BI commands.

## Refresh a semantic model

```powershell
$refresh = Invoke-FabricXPowerBIRefresh `
    -TokenProvider $powerBiProvider `
    -WorkspaceId $workspaceId `
    -SemanticModelId $semanticModelId `
    -Wait `
    -TimeoutMinutes 30
```

Mutation commands support `-WhatIf` and `-Confirm`. FabricClientX does not hide refresh cancellation inside discovery or polling operations.

## Load an OfficeIMO CSV into Fabric Warehouse

The PowerShell module includes the optional `FabricClientX.OfficeIMO` adapter. OfficeIMO.CSV supplies schema inference and a forward-only data reader; DbaClientX performs the Warehouse bulk load; FabricClientX can then refresh a semantic model.

```powershell
$sqlToken = Get-AzAccessToken `
    -ResourceUrl 'https://database.windows.net' `
    -AsSecureString

$warehouseOptions = New-FabricXWarehouseConnectionOptions `
    -AccessToken $sqlToken.Token `
    -ExpiresOn $sqlToken.ExpiresOn

Invoke-FabricXCsvWorkflow `
    -CsvPath .\sales.csv `
    -SourceName Sales `
    -WarehouseConnectionString $warehouseConnectionString `
    -WarehouseConnectionOptions $warehouseOptions `
    -DestinationTable dbo.Sales `
    -WhatIf
```

This integration does not add a Fabric dependency to OfficeIMO. .NET consumers that need only Fabric or Power BI clients can reference `FabricClientX.Core` or `FabricClientX.PowerBI` without installing `FabricClientX.OfficeIMO`.
