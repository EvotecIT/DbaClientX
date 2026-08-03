---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXBulkInsert
## SYNOPSIS
Invokes a provider-native DbaClientX bulk insert from tabular PowerShell input.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-DbaXBulkInsert -Provider <DbaXProvider> -ConnectionString <string> -DestinationTable <string> -InputObject <Object> [-BatchSize <Int32>] [-BulkCopyTimeout <Int32>] [-UseTransaction] [-PassThru] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Invokes a provider-native DbaClientX bulk insert from tabular PowerShell input.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $rows | Invoke-DbaXBulkInsert -Provider SqlServer -ConnectionString $connectionString -DestinationTable dbo.Import -PassThru
```

Converts pipeline input to a DataTable and writes it using the SQL Server bulk provider.

## PARAMETERS

### -BatchSize
Optional rows per provider bulk batch.

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
Optional provider bulk-copy timeout in seconds. SQLite does not support this option.

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
Provider connection string, or SQLite database path.

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
Destination table name.

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
Tabular input to write. Accepts DataTable, DataView, IDataReader, DataRow, hashtable, and object pipeline input.

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

### -PassThru
Returns a small result object with provider, destination table, and row count.

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

### -Provider
Provider used for the bulk insert.

```yaml
Type: DbaXProvider
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: SqlServer, PostgreSql, MySql, Oracle, SQLite

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -UseTransaction
Executes the bulk insert inside a provider transaction where supported.

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

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `System.Object`

## OUTPUTS

- `None`

## RELATED LINKS

- None
