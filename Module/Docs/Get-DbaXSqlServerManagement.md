---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# Get-DbaXSqlServerManagement
## SYNOPSIS
Gets SQL Server-specific management metadata without requiring SQL Server Management Objects.

## SYNTAX
### __AllParameterSets
```powershell
Get-DbaXSqlServerManagement [-Type] <DbaXSqlServerManagementType> [-ConnectionString] <string> [-Name <string>] [-Schema <string>] [-Table <string>] [-DestinationSchema <string>] [-DestinationTable <string>] [-RoleName <string>] [-MemberName <string>] [-PrincipalName <string>] [-IncludeDisabled] [-IncludeSystem] [-IncludeAdvanced] [<CommonParameters>]
```

## DESCRIPTION
Returns SQL Server Agent, security, dependency, scripting, copy-plan, inventory, instance property, and configuration metadata using native SQL Server catalog queries.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> Get-DbaXSqlServerManagement -Type AgentJob -ConnectionString 'Server=.;Database=msdb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True'
```

Lists SQL Server Agent jobs visible through the supplied connection.

### EXAMPLE 2
```powershell
PS> Get-DbaXSqlServerManagement -Type DatabasePrincipal -ConnectionString 'Server=.;Database=AppDb;Integrated Security=True;Encrypt=True;TrustServerCertificate=True'
```

Lists database principals in the current database.

## PARAMETERS

### -ConnectionString
Specifies a SQL Server connection string.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 1
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -DestinationSchema
Destination schema name for table copy plans.

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

### -DestinationTable
Destination table name for table copy plans.

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

### -IncludeAdvanced
Includes advanced SQL Server configuration values.

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

### -IncludeDisabled
Includes disabled SQL Server Agent jobs or schedules where applicable.

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

### -IncludeSystem
Includes system principals where applicable.

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

### -MemberName
Optional member name filter for role membership metadata.

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

### -Name
Optional name filter used by Agent jobs, principals, instance properties, and configurations.

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

### -PrincipalName
Optional grantee principal name filter for permission metadata.

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

### -RoleName
Optional role name filter for role membership metadata.

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

### -Schema
Optional schema filter used by dependency, scripting, and copy-plan metadata.

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
Source table name for table copy plans.

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
Selects the SQL Server management metadata type to return.

```yaml
Type: DbaXSqlServerManagementType
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: AgentJob, AgentJobStep, AgentSchedule, ServerPrincipal, DatabasePrincipal, RoleMembership, Permission, InstanceProperty, Configuration, Dependency, ModuleScript, TableScript, TableCopyPlan, Inventory

Required: True
Position: 0
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
