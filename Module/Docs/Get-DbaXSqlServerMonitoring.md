---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXSqlServerMonitoring
## SYNOPSIS
Collects a SQL Server monitoring snapshot through the DbaClientX SQL Server provider.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXSqlServerMonitoring -Server <string> [-Database <string>] [-Scope <SqlServerMonitoringScope>] [-Port <Int32>] [-Username <string>] [-Password <string>] [-Credential <pscredential>] [-TrustServerCertificate] [-IncludeSystemDatabases] [-IncludeDisabledAgentJobs] [-MaxFullBackupAgeHours <Double>] [-MaxDifferentialBackupAgeHours <Double>] [-MaxLogBackupAgeMinutes <Double>] [-MaxCheckDbAgeDays <Double>] [-WaitStatisticThresholdPercent <decimal>] [-ConnectTimeoutSeconds <Int32>] [-ApplicationName <string>] [<CommonParameters>]
```

## DESCRIPTION
Collects a SQL Server monitoring snapshot through the DbaClientX SQL Server provider.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXSqlServerMonitoring -Server . -Database master -TrustServerCertificate
```

Returns a typed monitoring snapshot for the local SQL Server instance.

## PARAMETERS

### -ApplicationName
Optional SQL Server application name.

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

### -ConnectTimeoutSeconds
Optional connection timeout in seconds.

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

### -Credential
Optional SQL credential.

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
Database used for connection-level checks.

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

### -IncludeDisabledAgentJobs
Includes disabled SQL Server Agent jobs.

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

### -IncludeSystemDatabases
Includes system databases in database-level collectors.

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

### -MaxCheckDbAgeDays
Maximum age in days for last known good CHECKDB before status is considered overdue.

```yaml
Type: Double
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -MaxDifferentialBackupAgeHours
Maximum age in hours for differential backups before status is considered overdue.

```yaml
Type: Double
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -MaxFullBackupAgeHours
Maximum age in hours for full backups before status is considered overdue.

```yaml
Type: Double
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -MaxLogBackupAgeMinutes
Maximum age in minutes for log backups before status is considered overdue.

```yaml
Type: Double
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
Optional SQL login password.

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

### -Port
Optional TCP port.

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

### -Scope
Monitoring areas to collect.

```yaml
Type: SqlServerMonitoringScope
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: None, Connectivity, DatabaseState, BackupFreshness, CheckDbFreshness, AgentJobs, Baseline, WaitStatistics, AvailabilityGroups, All

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Server
SQL Server instance name or address.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: SqlInstance, Instance
Possible values:

Required: True
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -TrustServerCertificate
Trusts the SQL Server certificate while keeping encryption enabled.

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
Optional SQL login name.

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

### -WaitStatisticThresholdPercent
Maximum cumulative wait percentage to include in wait statistics.

```yaml
Type: Decimal
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
