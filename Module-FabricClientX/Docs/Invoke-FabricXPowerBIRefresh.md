---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-FabricXPowerBIRefresh
## SYNOPSIS
Requests a Power BI semantic-model refresh and optionally waits for settlement.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-FabricXPowerBIRefresh [-WorkspaceId] <guid> [-SemanticModelId] <guid> -TokenProvider <IFabricTokenProvider> [-Wait] [-TimeoutMinutes <int>] [-PollIntervalSeconds <int>] [-NotifyOption <string>] [-RetryCount <Int32>] [-OperationId <string>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Requests a Power BI semantic-model refresh and optionally waits for settlement.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Invoke-FabricXPowerBIRefresh -TokenProvider $powerBiProvider -WorkspaceId $workspaceId -SemanticModelId $modelId -Wait -TimeoutMinutes 30
```

Returns refresh identity, terminal status, and the stable OperationId.

## PARAMETERS

### -NotifyOption
Power BI notification option. Omit it for service-principal enhanced refreshes.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: NoNotification, MailOnFailure, MailOnCompletion

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

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

### -PollIntervalSeconds
Polling interval in seconds.

```yaml
Type: Int32
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -RetryCount
Optional Power BI service-side retry count.

```yaml
Type: Int32
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SemanticModelId
Semantic-model identifier.

```yaml
Type: Guid
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 1
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TimeoutMinutes
Maximum settlement wait in minutes.

```yaml
Type: Int32
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

### -Wait
Waits until the refresh reaches a terminal state.

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

- `FabricClientX.PowerBI.PowerBiRefreshStartResult`
- `FabricClientX.PowerBI.PowerBiRefreshSettlement`

## RELATED LINKS

- None
