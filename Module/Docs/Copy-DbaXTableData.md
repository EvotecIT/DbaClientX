---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Copy-DbaXTableData
## SYNOPSIS
Copies table data from one DbaClientX provider connection to another using paged reads and provider-native bulk writes.

## SYNTAX
### __AllParameterSets
```powershell
Copy-DbaXTableData -SourceProvider <DbaXBulkProvider> -SourceConnectionString <string> -DestinationProvider <DbaXBulkProvider> -DestinationConnectionString <string> [-SourceTable <string>] [-DestinationTable <string>] [-Definition <DbaTableCopyDefinition[]>] [-OrderBy <string[]>] [-AllowUnordered] [-PageSize <int>] [-BatchSize <Int32>] [-BulkCopyTimeout <Int32>] [-ColumnMap <hashtable>] [-ExcludeColumn <string[]>] [-BooleanColumn <string[]>] [-Int32Column <string[]>] [-Int64Column <string[]>] [-DecimalColumn <string[]>] [-StringColumn <string[]>] [-DateTimeColumn <string[]>] [-DeduplicateSourceBy <string[]>] [-DeduplicateSourceOrderBy <string[]>] [-DeduplicateSourceCaseInsensitive] [-TreatMissingTablesAsEmpty] [-AllowSameTableCopy] [-SourceFabricWarehouse] [-DestinationFabricWarehouse] [-ClearDestination] [-NoVerify] [-OperationId <string>] [-TableLock] [-CheckConstraints] [-FireTriggers] [-KeepIdentity] [-KeepNulls] [-PassThru] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Use this cmdlet for table-to-table imports, exports, and migrations across SQL Server, PostgreSQL, MySQL, Oracle, and SQLite. The reusable copy orchestration lives in DbaClientX.Core while this cmdlet supplies PowerShell-friendly parameters and progress output.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Copy-DbaXTableData -SourceProvider SQLite -SourceConnectionString 'Data Source=C:\Data\history.db' -SourceTable ProbeResults -DestinationProvider SqlServer -DestinationConnectionString 'Server=.;Database=History;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' -DestinationTable dbo.ProbeResults -OrderBy Id -PageSize 10000 -BatchSize 5000 -ClearDestination -PassThru
```

Reads the SQLite table in deterministic pages and writes each page through SQL Server bulk copy.

### EXAMPLE 2
```powershell
PS> Copy-DbaXTableData -SourceProvider SqlServer -SourceConnectionString $source -SourceTable staging.Customers -DestinationProvider PostgreSql -DestinationConnectionString $dest -DestinationTable public.customers -OrderBy CustomerId -PageSize 25000 -PassThru
```

Copies all rows from SQL Server into PostgreSQL using provider-native read and write paths.

### EXAMPLE 3
```powershell
PS> $definitions | Copy-DbaXTableData -SourceProvider SQLite -SourceConnectionString $source -DestinationProvider SqlServer -DestinationConnectionString $dest -BatchSize 5000 -PassThru
```

Runs reusable DbaTableCopyDefinition objects produced by .NET planning code or constructed directly in PowerShell.

## PARAMETERS

### -AllowSameTableCopy
Allows copying from and to the same provider database table. Use only when intentionally owning the consequences.

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

### -AllowUnordered
Allows paged copies without an explicit order. Use only for ad hoc copies where provider natural order is acceptable.

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
Optional number of rows per provider bulk-write batch.

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

### -BooleanColumn
Column names converted to Boolean values before bulk writing.

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

### -BulkCopyTimeout
Optional provider bulk-copy timeout in seconds. SQLite destinations do not support this option.

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
SQL Server destination option to check destination constraints during each bulk copy.

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

### -ClearDestination
Deletes destination table rows before copying source rows.

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
Mapping from source column names to destination column names.

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

### -DateTimeColumn
Column names converted to DateTime values before bulk writing.

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

### -DecimalColumn
Column names converted to Decimal values before bulk writing.

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

### -DeduplicateSourceBy
Source key columns used to keep one effective row per key before copying.

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

### -DeduplicateSourceCaseInsensitive
Uses case-insensitive source keys when deduplicating source rows.

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

### -DeduplicateSourceOrderBy
Source columns used to choose the winning row for each deduplicated source key.

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

### -Definition
Reusable table copy definitions to execute. Use this for multi-table plans generated by DbaClientX.Core.

```yaml
Type: DbaTableCopyDefinition[]
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: True (ByValue)
Accept wildcard characters: False
```

### -DestinationConnectionString
Connection string used to write destination rows.

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

### -DestinationFabricWarehouse
Applies Microsoft Fabric Warehouse TDS and direct bulk-copy compatibility validation to a SQL Server destination.

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

### -DestinationProvider
Provider used to write destination rows.

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

### -DestinationTable
Destination table name. Include schema or owner when required by the provider.

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

### -ExcludeColumn
Source column names excluded from destination pages before bulk writing.

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

### -FireTriggers
SQL Server destination option to fire insert triggers during each bulk copy.

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

### -Int32Column
Column names converted to Int32 values before bulk writing.

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

### -Int64Column
Column names converted to Int64 values before bulk writing.

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

### -KeepIdentity
SQL Server destination option to preserve identity values from the source data.

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
SQL Server destination option to preserve null values from the source data.

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

### -NoVerify
Skips source and destination row-count verification after the copy.

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

### -OperationId
Optional non-zero 32-character W3C trace identifier used to correlate this copy with downstream workflows.

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

### -OrderBy
Column names used to order paged source reads. Provide stable key columns for deterministic copies.

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

### -PageSize
Number of rows read from the source per page.

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
Writes a result object with copied table counts, verification state, and elapsed time.

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

### -SourceConnectionString
Connection string used to read source rows.

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

### -SourceFabricWarehouse
Applies Microsoft Fabric Warehouse TDS compatibility validation to a SQL Server source.

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

### -SourceProvider
Provider used to read source rows.

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

### -SourceTable
Source table name. Include schema or owner when required by the provider.

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

### -StringColumn
Column names converted to String values before bulk writing.

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

### -TableLock
SQL Server destination option to acquire a bulk update lock for the duration of each bulk copy.

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

### -TreatMissingTablesAsEmpty
Treats missing source or destination tables as empty during row counts and clear operations.

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

- `DBAClientX.DataMovement.DbaTableCopyDefinition[]`

## OUTPUTS

- `None`

## RELATED LINKS

- None
