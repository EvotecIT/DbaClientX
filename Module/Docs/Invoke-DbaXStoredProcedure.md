---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXStoredProcedure
## SYNOPSIS
Invokes a stored procedure through a DbaClientX provider.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-DbaXStoredProcedure -Provider <DbaXProvider> -ConnectionString <string> -Procedure <string> [-Parameters <hashtable>] [-UseTransaction] [-Stream] [-ReturnType <ReturnType>] [-QueryTimeout <int>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Invokes a stored procedure through a DbaClientX provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Invoke-DbaXStoredProcedure -Provider SqlServer -ConnectionString $connectionString -Procedure dbo.GetUsers -ReturnType PSObject
```

Executes the stored procedure and converts rows to PowerShell objects.

## PARAMETERS

### -ConnectionString
Provider connection string.

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
Optional procedure parameters.

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

### -Procedure
Stored procedure name.

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

### -Provider
Provider used to execute the stored procedure.

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
Controls the returned object format.

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

### -Stream
Streams result rows instead of buffering the result.

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
