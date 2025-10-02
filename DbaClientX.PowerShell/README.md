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

## Notes

- See per-provider README for connection guidance.
- For large result sets, prefer `-Stream` where supported.

