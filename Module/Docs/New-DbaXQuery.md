---
external help file: DbaClientX-help.xml
Module Name: DbaClientX
online version: https://github.com/EvotecIT/DbaClientX
schema: 2.0.0
---
# New-DbaXQuery
## SYNOPSIS
Creates SQL query-builder objects using the DbaClientX core query builder.

## SYNTAX
### __AllParameterSets
```powershell
New-DbaXQuery [-TableName] <string> [-Action <DbaXQueryAction>] [-Columns <string[]>] [-Values <IDictionary>] [-Set <IDictionary>] [-Where <IDictionary>] [-ConflictColumns <string[]>] [-UpsertUpdateOnly <string[]>] [-OrderBy <string[]>] [-OrderByDescending <string[]>] [-Compile] [-CompileWithParameters] [-Dialect <SqlDialect>] [-Limit <Int32>] [-Offset <Int32>] [-WhatIf] [-Confirm] [<CommonParameters>]
```

## DESCRIPTION
Builds SELECT, INSERT, UPDATE, DELETE, and UPSERT statements without duplicating SQL-generation logic in PowerShell.

Use -Compile for literal SQL output or -CompileWithParameters to return SQL plus an ordered parameter map.

## EXAMPLES

### EXAMPLE 1
```powershell
PS> New-DbaXQuery -TableName 'dbo.Users' -Columns Id,DisplayName
```

Returns a core query object targeting selected columns from dbo.Users.

### EXAMPLE 2
```powershell
PS> New-DbaXQuery -TableName 'dbo.Users' -Columns Id,DisplayName -Where @{ IsActive = $true } -OrderBy DisplayName -Limit 10 -Offset 20 -Compile
```

Outputs a SQL Server SELECT statement with a WHERE clause and OFFSET/FETCH pagination.

### EXAMPLE 3
```powershell
PS> New-DbaXQuery -Action Insert -TableName 'dbo.Users' -Values ([ordered]@{ Id = 42; DisplayName = 'Ada' }) -Compile
```

Outputs an INSERT statement using the DbaClientX core query compiler.

### EXAMPLE 4
```powershell
PS> New-DbaXQuery -Action Update -TableName 'dbo.Users' -Set @{ DisplayName = 'Ada Lovelace' } -Where @{ Id = 42 } -Compile
```

Outputs an UPDATE statement with values supplied by PowerShell hashtables.

### EXAMPLE 5
```powershell
PS> New-DbaXQuery -Action Delete -TableName 'dbo.Users' -Where @{ Id = 42 } -Compile
```

Outputs a DELETE statement scoped by the provided WHERE values.

### EXAMPLE 6
```powershell
PS> New-DbaXQuery -Action Upsert -Dialect PostgreSql -TableName 'public.users' -Values ([ordered]@{ id = 42; display_name = 'Ada'; email = 'ada@example.test' }) -ConflictColumns id -UpsertUpdateOnly display_name,email -Compile
```

Outputs a PostgreSQL INSERT ... ON CONFLICT statement. Use another -Dialect value for SQL Server, MySQL, SQLite, or Oracle compiler behavior.

### EXAMPLE 7
```powershell
PS> New-DbaXQuery -Action Update -Dialect PostgreSql -TableName 'public.users' -Set @{ display_name = 'Ada' } -Where @{ id = 42 } -CompileWithParameters
```

Returns an object with Sql, Parameters, and ParameterValues properties for callers that want to execute parameterized SQL later.

## PARAMETERS

### -Action
The query-builder operation to create.

```yaml
Type: DbaXQueryAction
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: Select, Insert, Update, Delete, Upsert

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Columns
Columns to include in a SELECT statement. When omitted, the core builder emits *.

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

### -Compile
Compiles the query to a SQL string.

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

### -CompileWithParameters
Compiles the query to SQL text and returns ordered parameter values.

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

### -ConflictColumns
Columns used to detect UPSERT conflicts.

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

### -Dialect
SQL dialect used when compiling the query.

```yaml
Type: SqlDialect
Parameter Sets: __AllParameterSets
Aliases: None
Possible values: SqlServer, PostgreSql, MySql, SQLite, Oracle

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Limit
Limits the number of returned rows.

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

### -Offset
Skips a number of rows before returning results.

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

### -OrderBy
Columns to add to ORDER BY in ascending order.

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

### -OrderByDescending
Columns to add to ORDER BY in descending order.

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

### -Set
Column/value pairs for UPDATE SET clauses.

```yaml
Type: IDictionary
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
Name of the table targeted by the query.

```yaml
Type: String
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: True
Position: 0
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -UpsertUpdateOnly
Columns updated during an UPSERT conflict. When omitted, the core builder updates all non-conflict insert columns.

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

### -Values
Column/value pairs for INSERT or UPSERT statements.

```yaml
Type: IDictionary
Parameter Sets: __AllParameterSets
Aliases: None
Possible values:

Required: False
Position: named
Default value: None
Accept pipeline input: False
Accept wildcard characters: False
```

### -Where
Column/value pairs added as equality predicates to the WHERE clause.

```yaml
Type: IDictionary
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

- [SELECT statement (Transact-SQL)](https://learn.microsoft.com/sql/t-sql/queries/select-transact-sql)
- [Project documentation](https://github.com/EvotecIT/DbaClientX)

## NOTES

### Note

The cmdlet does not connect to the database or validate table existence; it only builds query objects or SQL text.
