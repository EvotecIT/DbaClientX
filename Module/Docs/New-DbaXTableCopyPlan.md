---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# New-DbaXTableCopyPlan
## SYNOPSIS
Builds a table-copy plan from supplied DbaClientX metadata objects.

## SYNTAX
### __AllParameterSets
```powershell
New-DbaXTableCopyPlan -SourceTables <DbaTableInfo[]> -SourceColumns <DbaColumnInfo[]> [-SourceIndexes <DbaIndexInfo[]>] [-DestinationColumns <DbaColumnInfo[]>] [-Provider <DbaXProvider>] [-SourceSchema <string>] [-DestinationSchema <string>] [-IncludeViews] [-TableMappings <hashtable>] [-ColumnMappings <hashtable>] [-ExcludedColumns <string[]>] [-ColumnTypeConversions <hashtable>] [-IncludeDestinationIdentityColumns] [-IncludeSourceGeneratedColumns] [-IncludeDestinationGeneratedColumns] [-SkipDestinationColumnMatch] [<CommonParameters>]
```

## DESCRIPTION
Builds a table-copy plan from supplied DbaClientX metadata objects.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> New-DbaXTableCopyPlan -SourceTables $tables -SourceColumns $columns -Provider SqlServer -DestinationSchema archive
```

Calls the shared DbaClientX table-copy planner.

## PARAMETERS

### -ColumnMappings
Optional global source column to destination column mappings.

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
Optional global column type conversions.

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

### -DestinationColumns
Optional destination column metadata.

```yaml
Type: DbaColumnInfo[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -DestinationSchema
Destination schema used for generated destination names.

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

### -ExcludedColumns
Optional global excluded columns.

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

### -IncludeDestinationGeneratedColumns
Includes destination generated columns in generated pages.

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

### -IncludeDestinationIdentityColumns
Includes destination identity columns in generated pages.

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

### -IncludeSourceGeneratedColumns
Includes source generated columns in generated pages.

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

### -IncludeViews
Includes views in generated copy definitions.

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
Provider whose identifier folding rules should be used.

```yaml
Type: DbaXProvider
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: SqlServer, PostgreSql, MySql, Oracle, SQLite

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SkipDestinationColumnMatch
Does not require source columns to exist in destination metadata.

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

### -SourceColumns
Source column metadata.

```yaml
Type: DbaColumnInfo[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SourceIndexes
Optional source index metadata.

```yaml
Type: DbaIndexInfo[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -SourceSchema
Restricts source tables to a schema.

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

### -SourceTables
Source table metadata.

```yaml
Type: DbaTableInfo[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TableMappings
Optional source table to destination table mappings.

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

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `None`

## RELATED LINKS

- None
