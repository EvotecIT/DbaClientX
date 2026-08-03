---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-FabricXWorkspace
## SYNOPSIS
Gets Microsoft Fabric workspaces visible to the authenticated principal.

## SYNTAX
### __AllParameterSets
```powershell
Get-FabricXWorkspace -TokenProvider <IFabricTokenProvider> [-Role <string[]>] [-PreferWorkspaceSpecificEndpoint] [-OperationId <string>] [<CommonParameters>]
```

## DESCRIPTION
Gets Microsoft Fabric workspaces visible to the authenticated principal.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-FabricXWorkspace -TokenProvider $provider
```

Returns workspace objects with a stable OperationId property.

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

### -PreferWorkspaceSpecificEndpoint
Includes workspace-specific endpoints when the service supports them.

```yaml
Type: SwitchParameter
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Role
Optional workspace roles such as Admin, Member, Contributor, or Viewer.

```yaml
Type: String[]
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

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `FabricClientX.FabricWorkspace`

## RELATED LINKS

- None
