---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXSQLite
## SYNOPSIS
Invokes a query against a SQLite database.

## SYNTAX
### Query (Default)
```powershell
Invoke-DbaXSQLite -Database <string> -Query <string> [-QueryTimeout <int>] [-Stream] [-ReturnType <ReturnType>] [-Parameters <hashtable>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Executes SQL statements on a specified SQLite database and returns data in the format you choose.

Supports streaming results for large data sets when the platform allows asynchronous enumeration.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Invoke-DbaXSQLite -Database 'app.db' -Query 'SELECT * FROM Users'
```

Executes the query and outputs each row as a DataRow.

### EXAMPLE 2
```powershell
PS> Invoke-DbaXSQLite -Database 'app.db' -Query 'SELECT * FROM Logs' -Stream -ReturnType DataRow
```

Streams each row as it is received, which is useful for large result sets.

## PARAMETERS

### -Database
Specifies the path to the SQLite database file.

```yaml
Type: String
Parameter Sets: Query
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Parameters
Provides additional query parameters.

```yaml
Type: Hashtable
Parameter Sets: Query
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Query
Defines the SQL query to execute.

```yaml
Type: String
Parameter Sets: Query
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -QueryTimeout
Sets the command timeout in seconds.

```yaml
Type: Int32
Parameter Sets: Query
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -ReturnType
Selects the format of returned data.

```yaml
Type: ReturnType
Parameter Sets: Query
Aliases: As
Possible values: DataSet, DataTable, DataRow, PSObject

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Stream
Streams results instead of buffering them.

```yaml
Type: SwitchParameter
Parameter Sets: Query
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

- [SQLite in .NET](https://learn.microsoft.com/dotnet/standard/data/sqlite/)
- [Project documentation](https://github.com/EvotecIT/DbaClientX)

## NOTES

### Note

When -Stream is used on platforms without streaming support, a NotSupportedException is thrown.
