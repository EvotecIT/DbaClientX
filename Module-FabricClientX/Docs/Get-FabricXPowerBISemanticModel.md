---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-FabricXPowerBISemanticModel
## SYNOPSIS
Gets Power BI semantic models in a workspace.

## SYNTAX
### __AllParameterSets
```powershell
Get-FabricXPowerBISemanticModel [-WorkspaceId] <guid> -TokenProvider <IFabricTokenProvider> [-OperationId <string>] [<CommonParameters>]
```

## DESCRIPTION
Gets Power BI semantic models in a workspace.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-FabricXPowerBISemanticModel -TokenProvider $powerBiProvider -WorkspaceId $workspaceId | Where-Object IsRefreshable
```

Returns typed semantic models with a stable OperationId property.

## PARAMETERS

### -OperationId
Optional non-zero W3C trace identifier used to correlate the request.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
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

### -WorkspaceId
Power BI workspace identifier.

```yaml
Type: Guid
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 0
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `FabricClientX.PowerBI.PowerBiSemanticModel`

## RELATED LINKS

- None
