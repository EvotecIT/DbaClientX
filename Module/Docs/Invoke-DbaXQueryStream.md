---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXQueryStream
## SYNOPSIS
Streams query rows through a DbaClientX provider.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-DbaXQueryStream -Provider <DbaXProvider> -ConnectionString <string> -Query <string> [-Parameters <hashtable>] [-UseTransaction] [-ReturnType <ReturnType>] [-QueryTimeout <int>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Streams query rows through a DbaClientX provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Invoke-DbaXQueryStream -Provider SqlServer -ConnectionString $connectionString -Query 'SELECT name FROM sys.databases' -ReturnType PSObject
```

Streams query rows without buffering the full result set.

## PARAMETERS

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

### -Parameters
Optional query parameters.

```yaml
Type: Hashtable
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
Provider used to stream query rows.

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

### -Query
Query text to execute.

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

### -QueryTimeout
Optional command timeout in seconds.

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

### -ReturnType
Controls the returned object format. Defaults to PSObject so an ordinary PowerShell query emits every row.

```yaml
Type: ReturnType
Parameter Sets: __AllParameterSets
Aliases: As
Possible values: DataSet, DataTable, DataRow, PSObject

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -UseTransaction
Executes through an active provider transaction when supported.

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

- `None`

## OUTPUTS

- `None`

## RELATED LINKS

- None
