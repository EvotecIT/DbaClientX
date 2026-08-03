---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Test-DbaXConnection
## SYNOPSIS
Validates and optionally pings a DbaClientX provider connection string.

## SYNTAX
### __AllParameterSets
```powershell
Test-DbaXConnection [-Provider] <DbaXProvider> [-ConnectionString] <string> [-SkipPing] [-Detailed] [<CommonParameters>]
```

## DESCRIPTION
Validates and optionally pings a DbaClientX provider connection string.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Test-DbaXConnection -Provider SqlServer -ConnectionString 'Server=.;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True' -Detailed
```

Returns validation and ping details for the supplied SQL Server connection string.

## PARAMETERS

### -ConnectionString
Provider connection string, or a SQLite database path.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 1
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Detailed
Return a detailed result object instead of a Boolean.

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
Database provider used to validate the connection string.

```yaml
Type: DbaXProvider
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: SqlServer, PostgreSql, MySql, Oracle, SQLite

Required: True
Position: 0
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SkipPing
Only validate connection-string shape and skip opening a provider connection.

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
