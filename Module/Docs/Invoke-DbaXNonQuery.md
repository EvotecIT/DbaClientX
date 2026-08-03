---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Invoke-DbaXNonQuery
## SYNOPSIS
Executes a non-query SQL command against SQL Server.

## SYNTAX
### DefaultCredentials (Default)
```powershell
Invoke-DbaXNonQuery -Server <string> -Database <string> -Query <string> [-QueryTimeout <int>] [-Parameters <hashtable>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-TrustServerCertificate] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Runs an SQL statement such as INSERT, UPDATE, or DELETE and returns the number of affected rows.

Supports SQL authentication or integrated security based on provided credentials.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> $sql = @'
             CREATE TABLE #DbaClientXDemo
             (
                 Id int NOT NULL,
                 Name nvarchar(50) NOT NULL
             );

             INSERT INTO #DbaClientXDemo (Id, Name)
             VALUES (1, N'Alpha'), (2, N'Beta');
             '@

             Invoke-DbaXNonQuery -Server 'localhost' -Database 'master' -TrustServerCertificate -Query $sql
```

Executes a multi-line command and returns the number of affected rows.

### EXAMPLE 2
```powershell
PS> $credential = Get-Credential 'app_writer'
Invoke-DbaXNonQuery -Server 'sql01' -Database 'app' -Query 'UPDATE dbo.Users SET LastSeenUtc = SYSUTCDATETIME() WHERE Id = @Id' -Credential $credential -Parameters @{ Id = 42 }
```

Executes the statement using the supplied credentials.

## PARAMETERS

### -Credential
Optional SQL authentication credential.

```yaml
Type: PSCredential
Parameter Sets: DefaultCredentials
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Database
Defines the target database.

```yaml
Type: String
Parameter Sets: DefaultCredentials
Aliases: None
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Parameters
Provides parameters for the SQL command.

```yaml
Type: Hashtable
Parameter Sets: DefaultCredentials
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
Parameter Sets: DefaultCredentials
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Query
The SQL command to execute.

```yaml
Type: String
Parameter Sets: DefaultCredentials
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
Parameter Sets: DefaultCredentials
Aliases: None
Possible values:

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
Parameter Sets: DefaultCredentials
Aliases: DBServer, SqlInstance, Instance
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TrustServerCertificate
Trusts the SQL Server TLS certificate without validating the certificate chain.

```yaml
Type: SwitchParameter
Parameter Sets: DefaultCredentials
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
Parameter Sets: DefaultCredentials
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

Use caution with destructive statements; the cmdlet respects -WhatIf and -Confirm.
