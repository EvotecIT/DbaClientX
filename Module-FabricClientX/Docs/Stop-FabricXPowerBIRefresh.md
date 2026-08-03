---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Stop-FabricXPowerBIRefresh
## SYNOPSIS
Cancels an accepted Power BI semantic-model refresh after explicit confirmation.

## SYNTAX
### __AllParameterSets
```powershell
Stop-FabricXPowerBIRefresh [-Refresh] <PowerBiRefreshStartResult> -TokenProvider <IFabricTokenProvider> [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Cancels an accepted Power BI semantic-model refresh after explicit confirmation.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $refresh | Stop-FabricXPowerBIRefresh -TokenProvider $powerBiProvider -Confirm
```

Cancellation is never performed by discovery or settlement commands.

## PARAMETERS

### -Refresh
Accepted refresh returned by Invoke-FabricXPowerBIRefresh.

```yaml
Type: PowerBiRefreshStartResult
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 0
Default value: None
Accept pipeline input: True (ByValue)
Accept wildcard characters: False
```

### -TokenProvider
Caller-owned token provider configured for the Power BI API scope.

```yaml
Type: IFabricTokenProvider
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `FabricClientX.PowerBI.PowerBiRefreshStartResult`

## OUTPUTS

- `FabricClientX.FabricResponse`

## RELATED LINKS

- None
