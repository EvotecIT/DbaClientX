---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXAzureTableEntity
## SYNOPSIS
Reads Azure Table entities with native continuation-token paging.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXAzureTableEntity -ConnectionString <string> -TableName <string> [-Filter <string>] [-Select <string[]>] [-PageSize <int>] [-ContinuationToken <string>] [-AsPage] [<CommonParameters>]
```

## DESCRIPTION
Reads Azure Table entities with native continuation-token paging.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXAzureTableEntity -ConnectionString $connectionString -TableName Reports -Filter "PartitionKey eq 'daily'"
```

Streams matching entities while following Azure continuation tokens internally.

### EXAMPLE 2
```powershell
PS> Get-DbaXAzureTableEntity -ConnectionString $connectionString -TableName Reports -PageSize 500 -AsPage
```

Returns each page with its opaque continuation token.

## PARAMETERS

### -AsPage
Return page envelopes instead of enumerating individual entities.

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

### -ConnectionString
Azure Storage or Cosmos DB Table API connection string.

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

### -ContinuationToken
Optional token at which to resume the query.

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

### -Filter
Optional OData filter.

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

### -PageSize
Maximum entities requested from Azure per page.

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

### -Select
Optional property projection. PartitionKey and RowKey are always included.

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

### -TableName
Table to query.

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

- `DBAClientX.AzureTables.DbaAzureTableEntity`
- `DBAClientX.AzureTables.DbaAzureTablePage`

## RELATED LINKS

- None
