---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXProviderCapability
## SYNOPSIS
Gets the capabilities exposed by DbaClientX providers.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXProviderCapability [[-Provider] <DbaXProvider[]>] [<CommonParameters>]
```

## DESCRIPTION
Gets the capabilities exposed by DbaClientX providers.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXProviderCapability
```

Returns one object per supported provider.

## PARAMETERS

### -Provider
Optional provider filter.

```yaml
Type: DbaXProvider[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: SqlServer, PostgreSql, MySql, Oracle, SQLite

Required: False
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
