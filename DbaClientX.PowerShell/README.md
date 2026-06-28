# DbaClientX.PowerShell

Use this module when a script needs to query, write, or copy database data without carrying provider-specific ADO.NET code. Import the module, choose the provider, and pass the same tabular shapes you already use in PowerShell: objects, `DataTable`, `IDataReader`, or rows imported from another tool.

## Install

```powershell
Install-Module DbaClientX -Scope CurrentUser
Import-Module DbaClientX
```

## Query A Database

Use `-Stream` when you want to process large SQL Server results row by row:

```powershell
Invoke-DbaXQuery -Server 'localhost' -Database 'App' -Query 'SELECT TOP 5 Id,Name FROM dbo.Users' -Stream -ReturnType DataRow
```

Run a PostgreSQL update with named parameters:

```powershell
Invoke-DbaXPostgreSqlNonQuery -Server 'localhost' -Database 'app' -Username 'user' -Password 'p@ss' -Query "UPDATE users SET active=false WHERE id=@id" -Parameters @{ '@id' = 1 }
```

Load a SQLite table into a `DataTable` when another step needs tabular input:

```powershell
Invoke-DbaXSQLite -Database './app.db' -Query 'SELECT Id, Name FROM Users' -ReturnType DataTable
```

## Write Rows To A Table

Write PowerShell objects to SQL Server and create the staging table when it is missing:

```powershell
$rows = @(
    [pscustomobject]@{ Id = 1; DisplayName = 'Alice' }
    [pscustomobject]@{ Id = 2; DisplayName = 'Bob' }
)

$rows | Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'staging.Users' `
    -AutoCreateTable `
    -TableLock `
    -BatchSize 5000 `
    -PassThru
```

Import Excel rows with PSWriteOffice and let DbaClientX own the database write:

```powershell
Import-OfficeExcel .\Users.xlsx -AsDataTable |
    Write-DbaXTableData `
        -Provider PostgreSql `
        -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret;SslMode=Require' `
        -DestinationTable 'public.import_users' `
        -BatchSize 5000
```

## Copy A Table Between Providers

Use `Copy-DbaXTableData` when both sides are database tables and you want DbaClientX to read, page, write, and verify the copy:

```powershell
Copy-DbaXTableData `
    -SourceProvider SQLite `
    -SourceConnectionString 'Data Source=C:\Data\history.db' `
    -SourceTable 'ProbeResults' `
    -DestinationProvider SqlServer `
    -DestinationConnectionString 'Server=.;Database=History;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ProbeResults' `
    -OrderBy Id `
    -PageSize 10000 `
    -BatchSize 5000 `
    -TableLock `
    -ClearDestination `
    -PassThru
```

## Build SQL Before Running It

Build SQL through the core query builder when a script needs provider quoting but you still want to decide where to execute the command:

```powershell
New-DbaXQuery -TableName 'dbo.Users' -Columns Id,DisplayName -Where @{ IsActive = $true } -OrderBy DisplayName -Limit 10 -Compile
```

Generate INSERT, UPDATE, DELETE, and UPSERT statements:

```powershell
New-DbaXQuery -Action Insert -TableName 'dbo.Users' -Values ([ordered]@{ Id = 42; DisplayName = 'Ada' }) -Compile

New-DbaXQuery -Action Update -Dialect PostgreSql -TableName 'public.users' -Set @{ display_name = 'Ada Lovelace' } -Where @{ id = 42 } -Compile

New-DbaXQuery -Action Delete -Dialect PostgreSql -TableName 'public.users' -Where @{ id = 42 } -Compile

New-DbaXQuery -Action Upsert -Dialect PostgreSql -TableName 'public.users' -Values ([ordered]@{ id = 42; display_name = 'Ada'; email = 'ada@example.test' }) -ConflictColumns id -UpsertUpdateOnly display_name,email -Compile
```

Return parameterized SQL plus a named parameter map, then run it with the provider cmdlet:

```powershell
$compiled = New-DbaXQuery -Action Update -Dialect PostgreSql -TableName 'public.users' -Set @{ display_name = 'Ada' } -Where @{ id = 42 } -CompileWithParameters
Invoke-DbaXPostgreSqlNonQuery -Server 'localhost' -Database 'app' -Username 'user' -Password 'p@ss' -Query $compiled.Sql -Parameters $compiled.Parameters
```

## Notes

- For large result sets, prefer `-Stream` where supported.
- `New-DbaXQuery` builds query objects and SQL text only; execute SQL with the provider-specific `Invoke-DbaX*` cmdlets.
