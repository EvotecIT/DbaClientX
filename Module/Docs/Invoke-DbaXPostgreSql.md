---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXPostgreSql
## SYNOPSIS
Invokes commands against a PostgreSQL database.

## SYNTAX
### Query (Default)
```powershell
Invoke-DbaXPostgreSql -Server <string> -Database <string> -Query <string> [-QueryTimeout <int>] [-Stream] [-ReturnType <ReturnType>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

### StoredProcedure
```powershell
Invoke-DbaXPostgreSql -Server <string> -Database <string> -StoredProcedure <string> [-QueryTimeout <int>] [-ReturnType <ReturnType>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Connects to a PostgreSQL server and executes a query or stored procedure with optional parameters.

Results can be streamed or returned in DataRow, DataTable, DataSet or PSObject formats.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $credential = Get-Credential 'app_reader'
Invoke-DbaXPostgreSql -Server 'pg01' -Database 'app' -Credential $credential -Query @'
SELECT
    current_database() AS database_name,
    current_user AS connected_as,
    version() AS server_version;
'@
```

Executes the query and returns every row as a PowerShell object.

### EXAMPLE 2
```powershell
PS> $credential = Get-Credential 'app_writer'
Invoke-DbaXPostgreSql -Server 'pg01' -Database 'app' -Credential $credential -StoredProcedure 'refresh_stats' -ReturnType DataTable
```

Runs the stored procedure and outputs a DataTable.

## PARAMETERS

### -Credential
The credential for authentication.

```yaml
Type: PSCredential
Parameter Sets: Query, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Database
Defines the database name on the server.

```yaml
Type: String
Parameter Sets: Query, StoredProcedure
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Parameters
Supplies parameters for the query or stored procedure.

```yaml
Type: Hashtable
Parameter Sets: Query, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Password
The password for authentication.

```yaml
Type: String
Parameter Sets: Query, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Query
Provides the SQL query text to execute.

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
Parameter Sets: Query, StoredProcedure
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -ReturnType
Selects the format of returned data. Defaults to PSObject so an ordinary PowerShell query emits every row.

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
Specifies the PostgreSQL server to connect to.

```yaml
Type: String
Parameter Sets: Query, StoredProcedure
Aliases: DBServer, SqlInstance, Instance
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -StoredProcedure
Names the stored procedure to invoke.

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
Parameter Sets: Query
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Username
The user name for authentication.

```yaml
Type: String
Parameter Sets: Query, StoredProcedure
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

- [Npgsql provider on MS Learn](https://learn.microsoft.com/ef/core/providers/npgsql/)
- [Project documentation](https://github.com/EvotecIT/DbaClientX)

## NOTES

### Note

Credentials are transmitted to the server; ensure secure channels when running over a network.
