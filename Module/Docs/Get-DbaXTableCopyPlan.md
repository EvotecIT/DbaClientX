---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXTableCopyPlan
## SYNOPSIS
Discovers provider metadata and builds a DbaClientX table-copy plan.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXTableCopyPlan -Provider <DbaXProvider> -ConnectionString <string> [-DestinationConnectionString <string>] [-SourceSchema <string>] [-SourceTable <string>] [-DestinationSchema <string>] [-IncludeViews] [-TableMappings <hashtable>] [-ColumnMappings <hashtable>] [-ExcludedColumns <string[]>] [-ColumnTypeConversions <hashtable>] [-IncludeDestinationIdentityColumns] [-IncludeSourceGeneratedColumns] [-IncludeDestinationGeneratedColumns] [-SkipDestinationColumnMatch] [<CommonParameters>]
```

## DESCRIPTION
Discovers provider metadata and builds a DbaClientX table-copy plan.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXTableCopyPlan -Provider SqlServer -ConnectionString $source -DestinationConnectionString $destination -DestinationSchema archive
```

Discovers source and destination metadata, then calls the shared DbaClientX planner.

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

### -ConnectionString
Source provider connection string, or SQLite source database path.

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

### -DestinationConnectionString
Optional destination provider connection string, or SQLite destination database path.

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
Provider used to discover metadata.

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

### -SourceSchema
Restricts source metadata to a schema.

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

### -SourceTable
Restricts source metadata to a table.

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
