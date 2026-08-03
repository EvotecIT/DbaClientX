---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# New-DbaXTableCopyDefinition
## SYNOPSIS
Creates a DbaClientX table-copy definition.

## SYNTAX
### __AllParameterSets
```powershell
New-DbaXTableCopyDefinition -SourceName <string> -DestinationName <string> [-LogicalName <string>] [-OrderByColumns <string[]>] [-ColumnMappings <hashtable>] [-ExcludedColumns <string[]>] [-ColumnTypeConversions <hashtable>] [-DeduplicateByColumns <string[]>] [-DeduplicateOrderByColumns <string[]>] [-DeduplicateCaseInsensitive] [<CommonParameters>]
```

## DESCRIPTION
Creates a DbaClientX table-copy definition.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> New-DbaXTableCopyDefinition -SourceName dbo.Users -DestinationName archive.Users -OrderByColumns Id
```

Returns a typed copy definition that can be passed to DbaClientX data movement APIs.

## PARAMETERS

### -ColumnMappings
Optional source column to destination column mappings.

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

### -ColumnTypeConversions
Optional destination column type conversions.

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

### -DeduplicateByColumns
Optional source columns used for source-side deduplication.

```yaml
Type: String[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -DeduplicateCaseInsensitive
Uses case-insensitive source-side deduplication.

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

### -DeduplicateOrderByColumns
Optional source order columns used to choose rows during deduplication.

```yaml
Type: String[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -DestinationName
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

### -ExcludedColumns
Optional columns to exclude from copy pages.

```yaml
Type: String[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -LogicalName
Optional logical display name.

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

### -OrderByColumns
Optional source-side order columns for paged reads.

```yaml
Type: String[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SourceName
Source table or view name.

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

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `None`

## RELATED LINKS

- None
