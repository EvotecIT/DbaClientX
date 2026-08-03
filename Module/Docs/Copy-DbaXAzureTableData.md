---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Copy-DbaXAzureTableData
## SYNOPSIS
Copies Azure Table data between storage accounts or Table API endpoints.

## SYNTAX
### __AllParameterSets
```powershell
Copy-DbaXAzureTableData -SourceConnectionString <string> -DestinationConnectionString <string> -SourceTable <string> -DestinationTable <string> [-Filter <string>] [-Select <string[]>] [-PageSize <int>] [-BatchSize <int>] [-WriteMode <DbaAzureTableWriteMode>] [-ClearDestination] [-NoVerify] [-NoCreateTable] [-PassThru] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Copies Azure Table data between storage accounts or Table API endpoints.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Copy-DbaXAzureTableData -SourceConnectionString $source -DestinationConnectionString $destination -SourceTable Reports -DestinationTable ReportsArchive -Filter "PartitionKey eq 'daily'" -PassThru
```

Streams pages through the provider-neutral DbaClientX copy engine and verifies row counts.

## PARAMETERS

### -BatchSize
Maximum entities per same-partition destination transaction.

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

### -ClearDestination
Clear existing destination entities before copying.

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

### -DestinationConnectionString
Destination Azure Storage or Cosmos DB Table API connection string.

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

### -DestinationTable
Destination table.

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

### -Filter
Optional source OData filter.

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

### -NoCreateTable
Do not create the destination table when it is missing.

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

### -NoVerify
Skip source and destination row-count scans.

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

### -PageSize
Entities requested per source page.

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

### -PassThru
Return the copy result.

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

### -Select
Optional source property projection. PartitionKey and RowKey are always copied.

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

### -SourceConnectionString
Source Azure Storage or Cosmos DB Table API connection string.

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

### -SourceTable
Source table.

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

### -WriteMode
Destination write behavior.

```yaml
Type: DbaAzureTableWriteMode
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: Add, UpsertMerge, UpsertReplace

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

- `None`

## RELATED LINKS

- None
