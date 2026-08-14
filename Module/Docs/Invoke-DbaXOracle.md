---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXOracle
## SYNOPSIS
Invokes commands against an Oracle database.

## SYNTAX
### __AllParameterSets
```powershell
Invoke-DbaXOracle -Server <string> -Database <string> -Query <string> [-QueryTimeout <int>] [-Stream] [-ReturnType <ReturnType>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Connects to an Oracle server using provided credentials and executes a SQL query.

Results can be returned in different formats based on the ReturnType.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $credential = Get-Credential 'app_reader'
Invoke-DbaXOracle -Server 'oracle01' -Database 'ORCLPDB1' -Credential $credential -Query @'
SELECT
    USER AS ConnectedUser,
    SYS_CONTEXT('USERENV', 'SERVICE_NAME') AS ServiceName
FROM dual
'@
```

Returns every Oracle session context row as a PowerShell object.

### EXAMPLE 2
```powershell
PS> $credential = Get-Credential 'app_reader'
Invoke-DbaXOracle -Server 'oracle01' -Database 'ORCLPDB1' -Credential $credential -Query @'
SELECT owner, table_name
FROM all_tables
WHERE owner = USER
ORDER BY table_name
'@ -Stream
```

Streams rows without buffering the entire result.

## PARAMETERS

### -Credential
Credential for authentication.

```yaml
Type: PSCredential
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Database
Defines the name of the database.

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

### -Parameters
Provides additional parameters for the query.

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

### -Password
Password for authentication.

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

### -Query
The SQL statement to execute.

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

### -QueryTimeout
Sets the timeout for the command in seconds.

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

### -ReturnType
Selects the format of returned data. Defaults to PSObject so an ordinary PowerShell query emits every row.

```yaml
Type: ReturnType
Parameter Sets: __AllParameterSets
Aliases: As
Possible values: DataSet, DataTable, DataRow, PSObject

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Server
Specifies the Oracle server to connect to.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: DBServer, SqlInstance, Instance
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Stream
Streams results without buffering.

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

### -Username
User name for authentication.

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

### CommonParameters
This cmdlet supports the common parameters: -Debug, -ErrorAction, -ErrorVariable, -InformationAction, -InformationVariable, -OutVariable, -OutBuffer, -PipelineVariable, -Verbose, -WarningAction, and -WarningVariable. For more information, see [about_CommonParameters](http://go.microsoft.com/fwlink/?LinkID=113216).

## INPUTS

- `None`

## OUTPUTS

- `None`

## RELATED LINKS

- [Oracle provider documentation](https://learn.microsoft.com/dotnet/standard/data/sqlite/?tabs=netcore-cli)
- [Project documentation](https://github.com/EvotecIT/DbaClientX)

## NOTES

### Note

Network operations may incur latency; consider using -Stream for large result sets.
