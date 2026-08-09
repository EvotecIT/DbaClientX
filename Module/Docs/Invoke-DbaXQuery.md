---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXQuery
## SYNOPSIS
Invokes a SQL Server query or stored procedure.

## SYNTAX
### Query (Default)
```powershell
Invoke-DbaXQuery -Server <string> -Database <string> -Query <string> [-QueryTimeout <int>] [-Stream] [-ReturnType <ReturnType>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-TrustServerCertificate] [-WhatIf] [-Confirm] [<CommonParameters>]
```

### QueryReader
```powershell
Invoke-DbaXQuery -Server <string> -Database <string> -Query <string> -AsDataReader [-QueryTimeout <int>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-TrustServerCertificate] [-WhatIf] [-Confirm] [<CommonParameters>]
```

### StoredProcedure
```powershell
Invoke-DbaXQuery -Server <string> -Database <string> -StoredProcedure <string> [-QueryTimeout <int>] [-Stream] [-ReturnType <ReturnType>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-TrustServerCertificate] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Connects to a SQL Server instance using integrated security or supplied credentials and executes the specified command.

Supports streaming results, multiple buffered return formats, and transferring an owned data reader to a consuming API.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $rows = Invoke-DbaXQuery -Server 'localhost' -Database 'master' -TrustServerCertificate -Query @'
             SELECT
                 name,
                 database_id,
                 create_date
             FROM sys.databases
             WHERE database_id > 4
             ORDER BY name;
             '@

             $rows | Format-Table name, database_id, create_date
```

Executes a multi-line query against a local SQL Server instance and returns each row as a DataRow.

### EXAMPLE 2
```powershell
PS> $credential = Get-Credential 'app_reader'
Invoke-DbaXQuery -Server 'sql01' -Database 'app' -StoredProcedure 'dbo.usp_GetActiveUsers' -Credential $credential -ReturnType DataTable
```

Runs the stored procedure and outputs a DataTable.

### EXAMPLE 3
```powershell
PS> $reader = Invoke-DbaXQuery -Server 'sql01' -Database 'app' -Query 'SELECT * FROM dbo.Users' -AsDataReader
try {
    Export-OfficeCsv -InputObject $reader -Path .\Users.csv
} finally {
    $reader.Dispose()
}
```

Returns one live DbDataReader. The caller must dispose it after the consuming API finishes reading.

## PARAMETERS

### -AsDataReader
Returns one live reader that owns its command and connection until the caller disposes it.

```yaml
Type: SwitchParameter
Parameter Sets: QueryReader
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Credential
Optional SQL authentication credential.

```yaml
Type: PSCredential
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Database
Defines the database name.

```yaml
Type: String
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Parameters
Provides additional parameters for the query or procedure.

```yaml
Type: Hashtable
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Password
Optional password for SQL authentication.

```yaml
Type: String
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Query
The SQL statement to execute.

```yaml
Type: String
Parameter Sets: Query, QueryReader
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
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -ReturnType
Selects the type of returned objects.

```yaml
Type: ReturnType
Parameter Sets: Query, StoredProcedure
Aliases: As
Possible values: DataSet, DataTable, DataRow, PSObject

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Server
Specifies the SQL Server instance.

```yaml
Type: String
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: DBServer, SqlInstance, Instance
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -StoredProcedure
Name of the stored procedure to run.

```yaml
Type: String
Parameter Sets: StoredProcedure
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Stream
Streams results instead of buffering them.

```yaml
Type: SwitchParameter
Parameter Sets: Query, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TrustServerCertificate
Trusts the SQL Server TLS certificate without validating the certificate chain.

```yaml
Type: SwitchParameter
Parameter Sets: Query, QueryReader, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Username
Optional user name for SQL authentication.

```yaml
Type: String
Parameter Sets: Query, QueryReader, StoredProcedure
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

- [Using SqlClient](https://learn.microsoft.com/dotnet/framework/data/adonet/using-sqlclient)
- [Project documentation](https://github.com/EvotecIT/DbaClientX)

## NOTES

### Note

When -ErrorAction Stop is used, execution will terminate on the first error.
