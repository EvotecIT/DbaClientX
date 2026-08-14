---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXSQLiteDiagnostics
## SYNOPSIS
Collects SQLite file and database diagnostics through the DbaClientX SQLite provider.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXSQLiteDiagnostics [-Database] <string> [-BusyTimeoutMs <Int32>] [<CommonParameters>]
```

## DESCRIPTION
Collects SQLite file and database diagnostics through the DbaClientX SQLite provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXSQLiteDiagnostics -Database .\app.db
```

Returns file, integrity, journal, and SQLite version diagnostics.

## PARAMETERS

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

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `None`

## RELATED LINKS

- None
