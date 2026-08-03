---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Write-DbaXAzureTableEntity
## SYNOPSIS
Writes PowerShell pipeline objects to Azure Tables in partition-safe transactions.

## SYNTAX
### __AllParameterSets
```powershell
Write-DbaXAzureTableEntity -ConnectionString <string> -TableName <string> -InputObject <Object> [-WriteMode <DbaAzureTableWriteMode>] [-BatchSize <int>] [-NoCreateTable] [-PassThru] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Input must expose PartitionKey and RowKey properties. Each Azure transaction contains at most 100 entities and stays inside one partition.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $rows | Write-DbaXAzureTableEntity -ConnectionString $connectionString -TableName Reports -WriteMode UpsertReplace -PassThru
```

Creates the table when needed and replaces matching entities.

## PARAMETERS

### -BatchSize
Maximum entities per same-partition transaction.

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

### -ConnectionString
Azure Storage or Cosmos DB Table API connection string.

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

### -InputObject
Objects containing PartitionKey, RowKey, and optional entity properties.

```yaml
Type: Object
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: True (ByValue)
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

### -PassThru
Return a summary after the write.

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

### -TableName
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

### -WriteMode
Azure entity write mode.

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

- `System.Object`

## OUTPUTS

- `None`

## RELATED LINKS

- None
