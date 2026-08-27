---
external help file: FabricClientX-help.xml
Module Name: FabricClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-FabricXCsvWorkflow
## SYNOPSIS
Plans and executes an OfficeIMO CSV to Fabric Warehouse and optional Power BI workflow.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-FabricXCsvWorkflow [-CsvPath] <string> -SourceName <string> -WarehouseConnectionString <string> -WarehouseConnectionOptions <SqlServerConnectionOptions> -DestinationTable <string> [-CsvLoadOptions <CsvLoadOptions>] [-CsvReaderOptions <CsvDataReaderOptions>] [-BatchSize <Int32>] [-BulkCopyTimeout <Int32>] [-Refresh] [-PowerBiTokenProvider <IFabricTokenProvider>] [-WorkspaceId <Guid>] [-SemanticModelId <Guid>] [-Wait] [-TimeoutMinutes <int>] [-OperationId <string>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Plans and executes an OfficeIMO CSV to Fabric Warehouse and optional Power BI workflow.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Invoke-FabricXCsvWorkflow -CsvPath .\sales.csv -SourceName Sales -WarehouseConnectionString $warehouse -WarehouseConnectionOptions $warehouseOptions -DestinationTable dbo.Sales -Refresh -PowerBiTokenProvider $powerBiProvider -WorkspaceId $workspaceId -SemanticModelId $modelId -Wait
```

Creates a redacted plan before performing the confirmed Warehouse write and refresh.

## PARAMETERS

### -BatchSize
Optional SQL bulk-copy batch size.

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

### -BulkCopyTimeout
Optional SQL bulk-copy timeout in seconds. Specify 0 for no timeout.

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

### -CsvLoadOptions
Optional OfficeIMO CSV parsing options.

```yaml
Type: CsvLoadOptions
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -CsvPath
Path to an OfficeIMO-compatible CSV file.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 0
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -CsvReaderOptions
Optional OfficeIMO CSV data-reader projection options.

```yaml
Type: CsvDataReaderOptions
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -DestinationTable
Warehouse destination table.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -OperationId
Optional non-zero W3C trace identifier used across all workflow stages.

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

### -PowerBiTokenProvider
Caller-owned token provider configured for the Power BI API scope.

```yaml
Type: IFabricTokenProvider
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Refresh
Requests a Power BI semantic-model refresh after successful ingestion.

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

### -SemanticModelId
Power BI semantic-model identifier used when Refresh is selected.

```yaml
Type: Guid
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SourceName
Logical source name used in the redacted plan.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TimeoutMinutes
Maximum refresh settlement wait in minutes.

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

### -Wait
Waits for the requested refresh to settle.

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

### -WarehouseConnectionOptions
Caller-owned Fabric Warehouse connection and authentication options.

```yaml
Type: SqlServerConnectionOptions
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -WarehouseConnectionString
Fabric Warehouse connection string without embedded credentials.

```yaml
Type: String
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
Power BI workspace identifier used when Refresh is selected.

```yaml
Type: Guid
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
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

- `FabricClientX.OfficeIMO.CsvFabricWorkflowResult`

## RELATED LINKS

- None
