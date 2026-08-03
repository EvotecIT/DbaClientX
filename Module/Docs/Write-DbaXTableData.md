---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Write-DbaXTableData
## SYNOPSIS
Writes tabular pipeline input to a database table using provider-native bulk insert APIs.

## SYNTAX
### __AllParameterSets
```powershell
Write-DbaXTableData -Provider <DbaXBulkProvider> -ConnectionString <string> -DestinationTable <string> -InputObject <Object> [-BatchSize <Int32>] [-BulkCopyTimeout <Int32>] [-ColumnMap <hashtable>] [-TableLock] [-CheckConstraints] [-FireTriggers] [-KeepIdentity] [-KeepNulls] [-AutoCreateTable] [-NotifyAfter <Int32>] [-PassThru] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Accepts a DataTable, DataView, IDataReader, DataRow pipeline, or regular PowerShell objects and routes the resulting table to the selected DbaClientX provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Import-OfficeExcel .\Data.xlsx -AsDataTable | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable dbo.Import
```

Loads the workbook rows as a DataTable and sends them through the SQL Server bulk-copy provider.

### EXAMPLE 2
```powershell
PS> $rows | Write-DbaXTableData -Provider SqlServer -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable staging.Import -AutoCreateTable -TableLock
```

Creates the destination schema and table when needed, then writes the rows through SQL Server bulk copy.

### EXAMPLE 3
```powershell
PS> $rows | Write-DbaXTableData -Provider PostgreSql -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret;SslMode=Require' -DestinationTable public.import_data -BatchSize 5000
```

Converts the objects to a DataTable and writes them with the PostgreSQL COPY-backed provider.

## PARAMETERS

### -AutoCreateTable
SQL Server-only option to create the destination schema and table when they do not already exist.

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

### -BatchSize
Optional number of rows per provider bulk batch.

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

### -BulkCopyTimeout
Optional provider bulk-copy timeout in seconds. SQLite does not support this option.

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

### -CheckConstraints
SQL Server-only option to check destination constraints during the copy.

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

### -ColumnMap
SQL Server-only mapping from source column names to destination column names.

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
Provider connection string used for the bulk insert.

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

### -DestinationTable
Destination table name. Include schema or owner when required by the provider.

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

### -FireTriggers
SQL Server-only option to fire insert triggers during the copy.

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
Tabular input to write. Accepts DataTable, DataView, IDataReader, DataRow, hashtable, and object pipeline input.

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

### -KeepIdentity
SQL Server-only option to preserve identity values from the source data.

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

### -KeepNulls
SQL Server-only option to preserve null values from the source data.

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

### -NotifyAfter
SQL Server-only number of rows copied between progress updates.

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

### -PassThru
Writes a small result object with provider, destination table, and row count.

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
Database provider that should receive the bulk insert.

```yaml
Type: DbaXBulkProvider
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: SqlServer, PostgreSql, MySql, Oracle, SQLite

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TableLock
SQL Server-only option to acquire a bulk update lock for the duration of the copy.

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
