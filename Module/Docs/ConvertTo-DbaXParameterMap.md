---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# ConvertTo-DbaXParameterMap
## SYNOPSIS
Converts objects into provider parameter dictionaries using the DbaClientX parameter mapper.

## SYNTAX
### __AllParameterSets
```powershell
ConvertTo-DbaXParameterMap -InputObject <Object> -Map <hashtable> [-Ambient <hashtable>] [-EnumsAsString] [-PreserveDateTimeOffset] [<CommonParameters>]
```

## DESCRIPTION
Converts objects into provider parameter dictionaries using the DbaClientX parameter mapper.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> [pscustomobject]@{ UserName = 'Ada' } | ConvertTo-DbaXParameterMap -Map @{ UserName = '@UserName' }
```

Returns a dictionary containing the provider parameter name and value.

## PARAMETERS

### -Ambient
Optional ambient values used when the input object does not contain a mapped property.

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

### -EnumsAsString
Converts enum values to strings rather than their underlying numeric value.

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

### -InputObject
Input object whose properties should be mapped to provider parameters.

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

### -Map
Logical property name to provider parameter name map.

```yaml
Type: Hashtable
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -PreserveDateTimeOffset
Preserves DateTimeOffset values instead of converting them to UTC DateTime values.

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
