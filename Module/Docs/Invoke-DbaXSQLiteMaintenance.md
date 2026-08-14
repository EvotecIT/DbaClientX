---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXSQLiteMaintenance
## SYNOPSIS
Runs SQLite maintenance operations through the DbaClientX SQLite provider.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-DbaXSQLiteMaintenance [-Database] <string> [-Action] <DbaXSQLiteMaintenanceAction> [-Destination <string>] [-CheckpointMode <SqliteCheckpointMode>] [-BusyTimeoutMs <Int32>] [-SkipOptimize] [-PassThru] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Runs SQLite maintenance operations through the DbaClientX SQLite provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Invoke-DbaXSQLiteMaintenance -Database .\app.db -Action PrepareForShutdown
```

Runs the provider shutdown maintenance sequence.

## PARAMETERS

### -Action
Maintenance operation to execute.

```yaml
Type: DbaXSQLiteMaintenanceAction
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: Backup, Checkpoint, Optimize, PrepareForShutdown

Required: True
Position: 1
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -BusyTimeoutMs
Optional busy timeout in milliseconds.

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

### -CheckpointMode
Checkpoint mode used by Checkpoint and PrepareForShutdown.

```yaml
Type: SqliteCheckpointMode
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: Passive, Full, Restart, Truncate

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Database
SQLite database path or SQLite connection string.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: Path, ConnectionString
Possible values:

Required: True
Position: 0
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Destination
Destination database path for the Backup action.

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

### -PassThru
Returns a small completion object.

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

### -SkipOptimize
Skips PRAGMA optimize after checkpointing during PrepareForShutdown.

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
