---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXMetadata
## SYNOPSIS
Gets database metadata without requiring SQL Server Management Objects.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXMetadata [-Provider] <DbaXProvider> [-Type] <DbaXMetadataType> [-ConnectionString] <string> [-Schema <string>] [-Table <string>] [-ExcludeViews] [<CommonParameters>]
```

## DESCRIPTION
Returns provider-neutral metadata for databases, tables/views, columns, indexes, foreign keys, and routines using native catalog queries from the selected provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXMetadata -Provider SqlServer -Type Table -ConnectionString 'Server=.;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True'
```

Lists tables and views visible through the supplied SQL Server connection.

### EXAMPLE 2
```powershell
PS> Get-DbaXMetadata -Provider SQLite -Type Column -ConnectionString '.\app.db' -Table Users
```

Returns column metadata for the Users table in the SQLite database file.

## PARAMETERS

### -ConnectionString
Specifies a provider connection string, or a SQLite database path when Provider is SQLite.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 2
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -ExcludeViews
Excludes views when requesting table metadata.

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
Specifies the database provider.

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

### -Schema
Optional schema or owner filter where the selected provider supports schemas.

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

### -Table
Optional table filter for column, index, and foreign key metadata.

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

### -Type
Selects the metadata type to return.

```yaml
Type: DbaXMetadataType
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: Database, Table, Column, Index, ForeignKey, Routine

Required: True
Position: 1
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
