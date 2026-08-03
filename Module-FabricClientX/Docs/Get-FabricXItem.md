---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-FabricXItem
## SYNOPSIS
Gets items from a Microsoft Fabric workspace.

## SYNTAX
### __AllParameterSets
```powershell
Get-FabricXItem [-WorkspaceId] <guid> -TokenProvider <IFabricTokenProvider> [-Type <string>] [-OperationId <string>] [<CommonParameters>]
```

## DESCRIPTION
Gets items from a Microsoft Fabric workspace.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-FabricXItem -TokenProvider $provider -WorkspaceId $workspaceId -Type SemanticModel
```

Uses the Fabric Core Items API and follows all continuation pages.

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
Caller-owned token provider configured for the Fabric API scope.

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

### -Type
Optional Fabric item type, such as SemanticModel or Warehouse.

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

### -WorkspaceId
Workspace identifier.

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

- `FabricClientX.FabricItem`

## RELATED LINKS

- None
