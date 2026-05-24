# DbaClientX - Multi-provider database client for .NET and PowerShell

DbaClientX is a lightweight database client for .NET and PowerShell. It keeps the database-specific work in provider libraries and exposes a small PowerShell surface for pleasant data movement, query execution, transactions, and bulk writes.

## NuGet Packages

[![DBAClientX.Core](https://img.shields.io/nuget/v/DBAClientX.Core?label=DBAClientX.Core)](https://www.nuget.org/packages/DBAClientX.Core)
[![DBAClientX.SqlServer](https://img.shields.io/nuget/v/DBAClientX.SqlServer?label=SQL%20Server)](https://www.nuget.org/packages/DBAClientX.SqlServer)
[![DBAClientX.PostgreSql](https://img.shields.io/nuget/v/DBAClientX.PostgreSql?label=PostgreSQL)](https://www.nuget.org/packages/DBAClientX.PostgreSql)
[![DBAClientX.MySql](https://img.shields.io/nuget/v/DBAClientX.MySql?label=MySQL)](https://www.nuget.org/packages/DBAClientX.MySql)
[![DBAClientX.SQLite](https://img.shields.io/nuget/v/DBAClientX.SQLite?label=SQLite)](https://www.nuget.org/packages/DBAClientX.SQLite)
[![DBAClientX.Oracle](https://img.shields.io/nuget/v/DBAClientX.Oracle?label=Oracle)](https://www.nuget.org/packages/DBAClientX.Oracle)

## PowerShell Module

[![PowerShell Gallery version](https://img.shields.io/powershellgallery/v/DbaClientX.svg)](https://www.powershellgallery.com/packages/DbaClientX)
[![PowerShell Gallery preview](https://img.shields.io/powershellgallery/v/DbaClientX.svg?label=powershell%20gallery%20preview&colorB=yellow&include_prereleases)](https://www.powershellgallery.com/packages/DbaClientX)
[![PowerShell Gallery platforms](https://img.shields.io/powershellgallery/p/DbaClientX.svg)](https://www.powershellgallery.com/packages/DbaClientX)
[![PowerShell Gallery downloads](https://img.shields.io/powershellgallery/dt/DbaClientX.svg)](https://www.powershellgallery.com/packages/DbaClientX)

## Project Information

[![Test .NET](https://github.com/EvotecIT/DbaClientX/actions/workflows/test-dotnet.yml/badge.svg)](https://github.com/EvotecIT/DbaClientX/actions/workflows/test-dotnet.yml)
[![Test PowerShell](https://github.com/EvotecIT/DbaClientX/actions/workflows/test-powershell.yml/badge.svg)](https://github.com/EvotecIT/DbaClientX/actions/workflows/test-powershell.yml)
[![codecov](https://codecov.io/gh/EvotecIT/DbaClientX/branch/master/graph/badge.svg)](https://codecov.io/gh/EvotecIT/DbaClientX)
[![license](https://img.shields.io/github/license/EvotecIT/DbaClientX.svg)](https://github.com/EvotecIT/DbaClientX)

## Author and Community

[![Blog](https://img.shields.io/badge/Blog-evotec.xyz-2A6496.svg)](https://evotec.xyz/hub)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-pklys-0077B5.svg?logo=LinkedIn)](https://www.linkedin.com/in/pklys)
[![Discord](https://img.shields.io/discord/508328927853281280?style=flat-square&label=discord%20chat)](https://evo.yt/discord)

## What it's all about

DbaClientX is meant to be the thin, reusable database layer behind higher-level tooling. For example, PSWriteOffice can focus on fast Excel import/export while DbaClientX handles provider-specific database writes. The PowerShell module stays small and operator-friendly; the heavy database logic remains in C#.

Use it when you need:

- one codebase for SQL Server, PostgreSQL, MySQL, SQLite, and Oracle
- sync and async query execution
- parameterized commands and provider-specific parameter type preservation
- transaction helpers that commit on success and roll back on failure
- provider-native bulk insert paths for staging tables and direct table writes
- PowerShell cmdlets for quick scripts, scheduled jobs, and data movement

## Supported Providers

| Provider | NuGet package | PowerShell query cmdlets | Bulk write | Transaction helper |
| --- | --- | --- | --- | --- |
| SQL Server | [DBAClientX.SqlServer](https://www.nuget.org/packages/DBAClientX.SqlServer) | `Invoke-DbaXQuery`, `Invoke-DbaXNonQuery` | `Write-DbaXTableData -Provider SqlServer` | `Invoke-DbaXTransaction` |
| PostgreSQL | [DBAClientX.PostgreSql](https://www.nuget.org/packages/DBAClientX.PostgreSql) | `Invoke-DbaXPostgreSql`, `Invoke-DbaXPostgreSqlNonQuery` | `Write-DbaXTableData -Provider PostgreSql` | `Invoke-DbaXPostgreSqlTransaction` |
| MySQL | [DBAClientX.MySql](https://www.nuget.org/packages/DBAClientX.MySql) | `Invoke-DbaXMySql`, `Invoke-DbaXMySqlNonQuery`, `Invoke-DbaXMySqlScalar` | `Write-DbaXTableData -Provider MySql` | `Invoke-DbaXMySqlTransaction` |
| SQLite | [DBAClientX.SQLite](https://www.nuget.org/packages/DBAClientX.SQLite) | `Invoke-DbaXSQLite` | `Write-DbaXTableData -Provider SQLite` | `Invoke-DbaXSQLiteTransaction` |
| Oracle | [DBAClientX.Oracle](https://www.nuget.org/packages/DBAClientX.Oracle) | `Invoke-DbaXOracle`, `Invoke-DbaXOracleNonQuery`, `Invoke-DbaXOracleScalar` | `Write-DbaXTableData -Provider Oracle` | `Invoke-DbaXOracleTransaction` |

## Install

PowerShell:

```powershell
Install-Module DbaClientX -Scope CurrentUser
```

.NET:

```powershell
dotnet add package DBAClientX.Core
dotnet add package DBAClientX.SqlServer
dotnet add package DBAClientX.PostgreSql
dotnet add package DBAClientX.MySql
dotnet add package DBAClientX.SQLite
dotnet add package DBAClientX.Oracle
```

Install only the provider packages you need.

## PowerShell Usage

### Query SQL Server

```powershell
Invoke-DbaXQuery `
    -Server 'sql01' `
    -Database 'App' `
    -Query 'SELECT TOP (10) * FROM dbo.Users' `
    -Credential $Credential
```

### Query Other Providers

```powershell
Invoke-DbaXPostgreSql -Server 'pg01' -Database 'app' -Query 'select * from users limit 10' -Credential $Credential
Invoke-DbaXMySql -Server 'mysql01' -Database 'app' -Query 'select * from users limit 10' -Credential $Credential
Invoke-DbaXOracle -Server 'ora01' -Database 'service' -Query 'select * from users fetch first 10 rows only' -Credential $Credential
Invoke-DbaXSQLite -Database '.\app.db' -Query 'select * from users limit 10'
```

### Build SQL

```powershell
New-DbaXQuery -TableName 'dbo.Users' -Limit 50 -Compile
```

### Write Table Data

`Write-DbaXTableData` accepts `DataTable`, `DataView`, `IDataReader`, `DataRow`, hashtables, and regular PowerShell objects from the pipeline. This makes it useful for staging-table imports and direct table writes.

```powershell
$rows = @(
    [pscustomobject]@{ Id = 1; DisplayName = 'Alice' }
    [pscustomobject]@{ Id = 2; DisplayName = 'Bob' }
)

$rows | Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ImportUsers' `
    -BatchSize 5000 `
    -PassThru
```

With PSWriteOffice:

```powershell
Import-OfficeExcel .\Users.xlsx -AsDataTable |
    Write-DbaXTableData `
        -Provider PostgreSql `
        -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret' `
        -DestinationTable 'public.import_users' `
        -BatchSize 5000
```

### Use Transactions

```powershell
Invoke-DbaXTransaction -Server 'sql01' -Database 'App' -ScriptBlock {
    param($client)

    $client.ExecuteNonQuery(
        'sql01',
        'App',
        $true,
        'UPDATE dbo.Users SET IsActive = 1 WHERE Id = 1',
        $null,
        $true
    )
}
```

Transaction helpers honor `-WhatIf` and `-Confirm`, commit when the script block succeeds, roll back on failure, and dispose the provider client in `finally`.

## .NET Usage

### Query Data

```csharp
using DBAClientX;
using System.Data;

using var sqlServer = new SqlServer {
    ReturnType = ReturnType.DataTable
};

var result = await sqlServer.QueryAsync(
    "SQL1",
    "master",
    integratedSecurity: true,
    query: "SELECT TOP 1 * FROM sys.databases");

if (result is DataTable table) {
    foreach (DataRow row in table.Rows) {
        Console.WriteLine(row["name"]);
    }
}
```

### Bulk Insert

```csharp
using System.Data;
using DBAClientX;

using var sqlServer = new SqlServer();
var table = new DataTable();
table.Columns.Add("Id", typeof(int));
table.Columns.Add("Name", typeof(string));
table.Rows.Add(1, "Example");

sqlServer.BulkInsert(
    server: "SQL1",
    database: "App",
    integratedSecurity: true,
    table: table,
    destinationTable: "dbo.ImportUsers",
    batchSize: 1000,
    bulkCopyTimeout: 60);
```

### Build Connection Strings

```csharp
var sqlServer = DBAClientX.SqlServer.BuildConnectionString("SQL1", "App", integratedSecurity: true, ssl: true);
var postgres = DBAClientX.PostgreSql.BuildConnectionString("localhost", "app", "user", "password", ssl: true);
var mysql = DBAClientX.MySql.BuildConnectionString("localhost", "app", "user", "password", ssl: true);
var sqlite = DBAClientX.SQLite.BuildConnectionString("app.db");
```

### Query Builder

```csharp
using DBAClientX.QueryBuilder;

var query = new Query()
    .Select("*")
    .From("users")
    .Where("name", "Alice")
    .Where("age", ">", 30);

var (sql, parameters) = QueryBuilder.CompileWithParameters(query);
```

## Supported .NET Versions

| Component | Windows | Linux/macOS |
| --- | --- | --- |
| Provider libraries | .NET Framework 4.7.2, .NET 8.0, .NET 10.0 | .NET 8.0, .NET 10.0 |
| PowerShell binary module | .NET Framework 4.7.2, .NET 8.0 | .NET 8.0 |
| Examples | .NET 8.0, .NET 10.0 | .NET 8.0, .NET 10.0 |
| Benchmarks | .NET 8.0, .NET 10.0 | .NET 8.0, .NET 10.0 |

## Repository Structure

| Path | Purpose |
| --- | --- |
| [`DbaClientX.Core`](DbaClientX.Core) | Shared base client, retry logic, query builder, connection validation, invoker abstractions |
| [`DbaClientX.SqlServer`](DbaClientX.SqlServer) | SQL Server provider |
| [`DbaClientX.PostgreSql`](DbaClientX.PostgreSql) | PostgreSQL provider |
| [`DbaClientX.MySql`](DbaClientX.MySql) | MySQL provider |
| [`DbaClientX.SQLite`](DbaClientX.SQLite) | SQLite provider |
| [`DbaClientX.Oracle`](DbaClientX.Oracle) | Oracle provider |
| [`DbaClientX.PowerShell`](DbaClientX.PowerShell) | Binary cmdlets and PowerShell-facing helpers |
| [`Module`](Module) | PowerShell module manifest, script functions, examples, Pester tests, and build script |
| [`DbaClientX.Examples`](DbaClientX.Examples) | C# usage examples |
| [`DbaClientX.Tests`](DbaClientX.Tests) | xUnit tests |
| [`DbaClientX.Benchmarks`](DbaClientX.Benchmarks) | BenchmarkDotNet scenarios |
| [`Build`](Build) | Project release configuration |

## Examples

- PowerShell examples: [`Module/Examples`](Module/Examples)
- C# examples: [`DbaClientX.Examples`](DbaClientX.Examples)
- Benchmarks: [`DbaClientX.Benchmarks`](DbaClientX.Benchmarks)

Useful example files:

- [`Example.QuerySqlServer.ps1`](Module/Examples/Example.QuerySqlServer.ps1)
- [`Example.QueryPostgreSql.ps1`](Module/Examples/Example.QueryPostgreSql.ps1)
- [`Example.QueryMySql.ps1`](Module/Examples/Example.QueryMySql.ps1)
- [`Example.QueryOracle.ps1`](Module/Examples/Example.QueryOracle.ps1)
- [`Example.QuerySQLite.ps1`](Module/Examples/Example.QuerySQLite.ps1)
- [`BulkInsertExample.cs`](DbaClientX.Examples/BulkInsertExample.cs)
- [`TransactionExample.cs`](DbaClientX.Examples/TransactionExample.cs)
- [`ParameterizedQueryExample.cs`](DbaClientX.Examples/ParameterizedQueryExample.cs)

## Build and Test

```powershell
dotnet restore DbaClientX.sln
dotnet build DbaClientX.sln -c Release
dotnet test DbaClientX.sln -c Release --framework net8.0
```

PowerShell module tests:

```powershell
.\Module\DbaClientX.Tests.ps1
```

Package build:

```powershell
$env:RefreshPSD1Only = 'false'
.\Module\Build\Build-Module.ps1
Remove-Item Env:\RefreshPSD1Only
```

## Release Packaging

Package publishing is intentionally manual in this repository because releases are signed locally with the USB key certificate.

Generate a build plan:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -Plan $true
```

Build signed packages locally:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -Build $true -PublishNuget $false -PublishGitHub $false
```

Publish NuGet and GitHub together in one versioned run:

```powershell
pwsh.exe -NoLogo -NoProfile -File .\Build\Build-Project.ps1 -PublishNuget $true -PublishGitHub $true
```

Build configuration lives in [`Build/project.build.json`](Build/project.build.json), and artifacts are generated under `Artefacts/ProjectBuild`.

## Notes

- The solution enables nullable reference types and .NET analyzers via [`Directory.Build.props`](Directory.Build.props).
- SourceLink is enabled for provider projects for easier debugging into packages.
- SQL Server uses `Microsoft.Data.SqlClient`.
- PowerShell 7 package builds use a module-scoped AssemblyLoadContext so DbaClientX can coexist more safely with other modules that load overlapping assemblies.
- The release wrapper treats version updates as part of publishing. If you intentionally need a replay-only publish for already-versioned artifacts, pass `-UpdateVersions $false` explicitly.
