---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXSQLiteTransaction
## SYNOPSIS
Runs a script block inside a SQLite transaction.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-DbaXSQLiteTransaction -Database <string> -ScriptBlock <scriptblock> [-QueryTimeout <int>] [-IsolationLevel <IsolationLevel>] [-ArgumentList <Object[]>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Creates a SQLite client, begins a transaction, invokes the script block, and commits on success.

## EXAMPLES

### EXAMPLE 1
```powershell
Invoke-DbaXSQLiteTransaction -Database '.\app.db' -ScriptBlock { param($client) $client.ExecuteNonQuery('.\app.db', 'UPDATE Jobs SET Enabled = 1 WHERE Id = 42', $null, $true) }
```

Update a row in a SQLite transaction.

## PARAMETERS

### -ArgumentList
Additional arguments passed to the script block after the transaction client.

```yaml
Type: Object[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Database
Defines the SQLite database path or connection target.

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

### -IsolationLevel
Isolation level to use for the transaction.

```yaml
Type: IsolationLevel
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: Chaos, ReadUncommitted, ReadCommitted, RepeatableRead, Serializable, Snapshot, Unspecified

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -QueryTimeout
Command timeout to assign to the transaction client.

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

### -ScriptBlock
Script block executed with the transaction client as the first argument.

```yaml
Type: ScriptBlock
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

- `None`

## RELATED LINKS

- None
