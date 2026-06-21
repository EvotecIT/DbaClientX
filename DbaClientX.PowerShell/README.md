# DbaClientX.PowerShell

PowerShell cmdlets over DbaClientX providers for quick database automation.

## Install / Import

- Build locally: `dotnet build DbaClientX.PowerShell -c Release`
- Import from output: `Import-Module ./bin/Release/net8.0/DBAClientX.PowerShell.dll`

## Examples

Invoke a query against SQL Server and stream DataRow results:

```powershell
Invoke-DbaXQuery -Server 'localhost' -Database 'App' -Query 'SELECT TOP 5 Id,Name FROM dbo.Users' -Stream -ReturnType DataRow
```

Run a non-query against PostgreSQL:

```powershell
Invoke-DbaXPostgreSqlNonQuery -Server 'localhost' -Database 'app' -Username 'user' -Password 'p@ss' -Query "UPDATE users SET active=false WHERE id=@id" -Parameters @{ '@id' = 1 }
```

SQLite query returning a DataTable:

```powershell
Invoke-DbaXSQLite -Database './app.db' -Query 'SELECT Id, Name FROM Users' -ReturnType DataTable
```

Build SQL through the core query builder:

```powershell
New-DbaXQuery -TableName 'dbo.Users' -Columns Id,DisplayName -Where @{ IsActive = $true } -OrderBy DisplayName -Limit 10 -Compile
```

Generate INSERT, UPDATE, DELETE, and UPSERT statements without hand-writing provider quoting:

```powershell
New-DbaXQuery -Action Insert -TableName 'dbo.Users' -Values ([ordered]@{ Id = 42; DisplayName = 'Ada' }) -Compile

New-DbaXQuery -Action Update -Dialect PostgreSql -TableName 'public.users' -Set @{ display_name = 'Ada Lovelace' } -Where @{ id = 42 } -Compile

New-DbaXQuery -Action Delete -Dialect PostgreSql -TableName 'public.users' -Where @{ id = 42 } -Compile

New-DbaXQuery -Action Upsert -Dialect PostgreSql -TableName 'public.users' -Values ([ordered]@{ id = 42; display_name = 'Ada'; email = 'ada@example.test' }) -ConflictColumns id -UpsertUpdateOnly display_name,email -Compile
```

Return parameterized SQL plus a named parameter map:

```powershell
$compiled = New-DbaXQuery -Action Update -Dialect PostgreSql -TableName 'public.users' -Set @{ display_name = 'Ada' } -Where @{ id = 42 } -CompileWithParameters
Invoke-DbaXPostgreSqlNonQuery -Server 'localhost' -Database 'app' -Username 'user' -Password 'p@ss' -Query $compiled.Sql -Parameters $compiled.Parameters
```

## Notes

- See per-provider README for connection guidance.
- For large result sets, prefer `-Stream` where supported.
- `New-DbaXQuery` builds query objects and SQL text only; execute SQL with the provider-specific `Invoke-DbaX*` cmdlets.
