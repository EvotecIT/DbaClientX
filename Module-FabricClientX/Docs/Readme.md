---
Module Name: FabricClientX
Module Guid: ac8e01db-4199-4b08-a176-a1a6d3d23a88
Download Help Link: https://github.com/EvotecIT/DbaClientX
Help Version: 0.1.0
Locale: en-US
---
# FabricClientX Module
## Description
Microsoft Fabric and Power BI automation for PowerShell

## FabricClientX Cmdlets
### [Get-FabricXItem](Get-FabricXItem.md)
Gets items from a Microsoft Fabric workspace.

### [Get-FabricXPowerBISemanticModel](Get-FabricXPowerBISemanticModel.md)
Gets Power BI semantic models in a workspace.

### [Get-FabricXWorkspace](Get-FabricXWorkspace.md)
Gets Microsoft Fabric workspaces visible to the authenticated principal.

### [Invoke-FabricXCsvWorkflow](Invoke-FabricXCsvWorkflow.md)
Plans and executes an OfficeIMO CSV to Fabric Warehouse and optional Power BI workflow.

### [Invoke-FabricXPowerBIRefresh](Invoke-FabricXPowerBIRefresh.md)
Requests a Power BI semantic-model refresh and optionally waits for settlement.

### [New-FabricXTokenProvider](New-FabricXTokenProvider.md)
Creates a short-lived FabricClientX token provider from a caller-acquired secure token.

### [New-FabricXWarehouseConnectionOptions](New-FabricXWarehouseConnectionOptions.md)
Creates Fabric Warehouse connection options from a caller-acquired secure SQL token.

### [Stop-FabricXPowerBIRefresh](Stop-FabricXPowerBIRefresh.md)
Cancels an accepted Power BI semantic-model refresh after explicit confirmation.
