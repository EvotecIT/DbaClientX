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

DbaClientX is meant to be the thin, reusable database layer behind higher-level tooling. It owns provider connections, query execution, transactions, metadata, and bulk-copy behavior. PSWriteOffice and OfficeIMO own CSV and Excel parsing, writing, compression, and file-format rules. Together they form the intended file/database pair: PSWriteOffice shapes rows from files, DbaClientX moves those rows into or out of databases, and the boundary stays a normal `DataTable`, `IDataReader`, or object stream.

The PowerShell module stays small and operator-friendly; the heavy database logic remains in C#. That keeps user scripts clean without forcing people to pick low-level parser settings just to get good performance.

Use it when you need:

- one codebase for SQL Server, PostgreSQL, MySQL, SQLite, and Oracle
- sync and async query execution
- parameterized commands and provider-specific parameter type preservation
- transaction helpers that commit on success and roll back on failure
- provider-native bulk insert paths for staging tables and direct table writes
- provider-neutral metadata discovery for databases, tables, views, columns, indexes, foreign keys, and routines without SQL Server Management Objects
- PowerShell cmdlets for quick scripts, scheduled jobs, and data movement

## Supported Providers

| Provider | NuGet package | PowerShell query cmdlets | Bulk write | Transaction helper |
| --- | --- | --- | --- | --- |
| SQL Server | [DBAClientX.SqlServer](https://www.nuget.org/packages/DBAClientX.SqlServer) | `Invoke-DbaXQuery`, `Invoke-DbaXNonQuery` | `Write-DbaXTableData -Provider SqlServer` | `Invoke-DbaXTransaction` |
| PostgreSQL | [DBAClientX.PostgreSql](https://www.nuget.org/packages/DBAClientX.PostgreSql) | `Invoke-DbaXPostgreSql`, `Invoke-DbaXPostgreSqlNonQuery` | `Write-DbaXTableData -Provider PostgreSql` | `Invoke-DbaXPostgreSqlTransaction` |
| MySQL | [DBAClientX.MySql](https://www.nuget.org/packages/DBAClientX.MySql) | `Invoke-DbaXMySql`, `Invoke-DbaXMySqlNonQuery`, `Invoke-DbaXMySqlScalar` | `Write-DbaXTableData -Provider MySql` | `Invoke-DbaXMySqlTransaction` |
| SQLite | [DBAClientX.SQLite](https://www.nuget.org/packages/DBAClientX.SQLite) | `Invoke-DbaXSQLite` | `Write-DbaXTableData -Provider SQLite` | `Invoke-DbaXSQLiteTransaction` |
| Oracle | [DBAClientX.Oracle](https://www.nuget.org/packages/DBAClientX.Oracle) | `Invoke-DbaXOracle`, `Invoke-DbaXOracleNonQuery`, `Invoke-DbaXOracleScalar` | `Write-DbaXTableData -Provider Oracle` | `Invoke-DbaXOracleTransaction` |

## Data Movement

Start with the job you need to finish:

| You need to... | Use this | Keep this owner boundary |
| --- | --- | --- |
| Write PowerShell objects, `DataTable`, `DataView`, `IDataReader`, or Excel-imported rows to a table | `Write-DbaXTableData` | DbaClientX provider packages own the database write |
| Load a SQL Server staging table and let the command create the table, map columns, lock the table, preserve identities/nulls, fire triggers, check constraints, or report progress | `Write-DbaXTableData -Provider SqlServer` | SQL Server-specific knobs map to `SqlServerBulkInsertOptions` |
| Copy one or more tables between database providers | `Copy-DbaXTableData` | `DBAClientX.Core` owns the copy contracts/runner; provider packages own concrete adapters |
| Export SQL rows to CSV, compressed CSV, or Excel | `Invoke-DbaXQuery -ReturnType DataTable` plus PSWriteOffice `Export-OfficeCsv` / `Export-OfficeExcel` | DbaClientX owns the database read; PSWriteOffice/OfficeIMO owns file writing |
| Import CSV, compressed CSV, or Excel into SQL Server | PSWriteOffice `Import-OfficeCsv -AsDataTable` / `Import-OfficeExcel -AsDataTable` plus `Write-DbaXTableData` | PSWriteOffice/OfficeIMO owns file parsing; DbaClientX owns the database write |
| Stream a reader into SQL Server bulk copy | `Write-DbaXTableData -Provider SqlServer -InputObject (, $reader)` | The producer owns the reader; DbaClientX streams it into `SqlBulkCopy` |

The CSV round-trip path requires a PSWriteOffice build that exposes `Export-OfficeCsv` and `Import-OfficeCsv -AsDataTable`. Compressed CSV, null/date formatting, duplicate-header behavior, quote parsing, and static metadata columns require the newer PSWriteOffice CSV surface backed by OfficeIMO.CSV. When those CSV cmdlets are not available, use the Excel lane or pass `-PSWriteOfficeModulePath` to a compatible local PSWriteOffice build.

The PowerShell layer is intentionally thin: it maps friendly parameters to provider-owned C# APIs. Put repeatable database behavior in DbaClientX and keep consumer scripts focused on choosing source data, destination names, and credentials.

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

### Discover Metadata

```powershell
Get-DbaXMetadata `
    -Provider SqlServer `
    -Type Table `
    -ConnectionString 'Server=.;Database=master;Encrypt=True;TrustServerCertificate=True;Integrated Security=True'

Get-DbaXMetadata `
    -Provider SQLite `
    -Type Column `
    -ConnectionString '.\app.db' `
    -Table Users

Get-DbaXMetadata `
    -Provider PostgreSql `
    -Type ForeignKey `
    -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret' `
    -Schema public

Get-DbaXMetadata `
    -Provider Oracle `
    -Type Routine `
    -ConnectionString 'User Id=app;Password=secret;Data Source=localhost/XEPDB1'
```

### Write Table Data

Use `Write-DbaXTableData` when the data is already in PowerShell and the next step is a database table. The command accepts `DataTable`, `DataView`, `IDataReader`, `DataRow`, hashtables, and regular objects from the pipeline. SQL Server `IDataReader` input streams directly into `SqlBulkCopy`; when passing a reader through `-InputObject`, wrap it as `-InputObject (, $reader)` so PowerShell treats it as one object.

```powershell
$rows = @(
    [pscustomobject]@{ Id = 1; DisplayName = 'Alice' }
    [pscustomobject]@{ Id = 2; DisplayName = 'Bob' }
)

$rows | Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ImportUsers' `
    -AutoCreateTable `
    -BatchSize 5000 `
    -PassThru
```

Import rows from CSV or Excel with PSWriteOffice, then hand the `DataTable` to DbaClientX for the database write:

```powershell
Import-OfficeCsv .\Users.csv -AsDataTable |
    Write-DbaXTableData `
        -Provider PostgreSql `
        -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret' `
        -DestinationTable 'public.import_users' `
        -BatchSize 5000
```

Export SQL rows to CSV or compressed CSV by keeping the database read and file write in their owning libraries:

```powershell
$rows = Invoke-DbaXQuery `
    -Server 'sql01' `
    -Database 'App' `
    -Query 'SELECT Id, DisplayName, CreatedUtc FROM dbo.Users' `
    -ReturnType DataTable `
    -TrustServerCertificate

$rows | Export-OfficeCsv -Path .\Users.csv
$rows | Export-OfficeCsv -Path .\Users.csv.gz -CompressionType GZip
```

For the fastest SQL Server to CSV export path, stream an `IDataReader` from DbaClientX directly into PSWriteOffice and dispose the reader when the file writer is done:

```powershell
$connectionString = [DBAClientX.SqlServer]::BuildConnectionString(
    'sql01',
    'App',
    $true,
    $null,
    $null,
    $null,
    $null,
    $true)

$client = [DBAClientX.SqlServer]::new()
$reader = $null
try {
    $reader = $client.QueryReader($connectionString, 'SELECT Id, DisplayName, CreatedUtc FROM dbo.Users')
    Export-OfficeCsv -InputObject $reader -Path .\Users.csv
} finally {
    if ($null -ne $reader) {
        $reader.Dispose()
    }

    $client.Dispose()
}
```

Load compressed CSV back into SQL Server with the same table-write command:

```powershell
Import-OfficeCsv .\Users.csv.gz -CompressionType GZip -AsDataTable |
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsers' `
        -AutoCreateTable `
        -BatchSize 5000
```

The same database write path accepts Excel-imported tables:

```powershell
Import-OfficeExcel .\Users.xlsx -AsDataTable |
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsers' `
        -AutoCreateTable `
        -TableLock
```

Run the full SQL Server -> file -> SQL Server examples when you want to prove both sides together:

```powershell
.\Module\Examples\Example.CsvRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts
.\Module\Examples\Example.ExcelRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts
```

The examples create SQL Server source rows, export them to a `.csv` or `.xlsx` file with PSWriteOffice, import the file back as a `DataTable`, write it to SQL Server with `-AutoCreateTable`, and fail if any row count does not match.

When another library already gives you an `IDataReader`, pass the reader as one object so the SQL Server path stays streaming:

```powershell
Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ImportUsers' `
    -InputObject (, $reader) `
    -BatchSize 5000 `
    -TableLock
```

When the destination table has SQL Server-specific requirements, opt into the needed knobs explicitly:

```powershell
$customerTable | Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=sql01;Database=warehouse;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'staging.Customers' `
    -AutoCreateTable `
    -ColumnMap @{ CustomerName = 'DisplayName'; CustomerId = 'Id' } `
    -TableLock `
    -KeepIdentity `
    -KeepNulls `
    -NotifyAfter 5000 `
    -PassThru
```

Keep file-format conversion in the owning library. DbaClientX does small PowerShell input shaping: `TimeSpan` values stay scalar, scalar pipeline input becomes a `Value` column, and a single enumerable input expands into rows. For richer Excel, CSV, JSON, or document rules, shape the data first and pass DbaClientX a `DataTable`, `IDataReader`, or object stream.

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

## SQL Server Benchmark Snapshot

Use the SQL Server data-movement benchmark when you want repeatable local evidence for import performance. The benchmark is a PSPublishModule/PowerForge suite: DbaClientX only declares scenarios, provider engines, validation, and the README target; the shared benchmark runner owns timing, warmups, rotated ordering, comparison tables, README block updates, and JSON/CSV/Markdown artifacts.

```powershell
Install-Module PSPublishModule -MinimumVersion 3.0.44 -Scope CurrentUser

.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000, 5000, 20000, 100000 `
    -BatchSize 5000 `
    -InputKind DataTable, DataReader, PSCustomObject, Class `
    -Iterations 3
```

The suite benchmarks SQL Server writes and reads separately. The write suite covers DbaClientX across `DataTable`, `DataReader`, `PSCustomObject`, and typed class input shapes, and adds dbatools and SqlServer module lanes only when `Write-DbaDbTableData` or `Write-SqlTableData` are available. The `DataReader` lane is DbaClientX-only because it measures the public streaming path into `SqlBulkCopy`; dbatools and native SqlServer module comparisons remain on their documented client-side input shapes. The dbatools `DataTable` write lane uses a direct value passed to `-InputObject` so it stays on the documented SqlBulkCopy fast path instead of the slower piped-`DataRow` path. `Copy-DbaDbTableData` is intentionally not part of this matrix because it measures SQL table-to-table streaming rather than client-side object/DataTable import.

The read suite seeds an isolated SQL Server table outside the measured operation, then compares DbaClientX `Invoke-DbaXQuery` with dbatools `Invoke-DbaQuery` when dbatools is available. By default it reads every row as both `DataTable` and PowerShell-object output; `DataSet` can be selected with `-ReadShape DataSetAll` for local diagnosis. Successful lanes verify row counts plus simple data integrity (`Id` min/max/sum and `Score` sum) before dropping their isolated tables; failed lanes leave their table behind for inspection.

By default the wrapper imports the installed `DbaClientX` module. Use `-ModulePath`, `$env:DBACLIENTX_BENCHMARK_MODULE_PATH`, or `$env:DBACLIENTX_DEVELOPMENT_PATH` when benchmarking a local source build.

The checked-in snapshot below uses `DataTable`, `DataReader`, `PSCustomObject`, and typed class write input plus full-result `DataTable` and `PSObject` reads at 1k, 5k, 20k, and 100k rows. Pass additional `-ReadShape` values for a broader local read matrix.

The suite rewrites the marker-delimited tables below when it runs from a source checkout. Artifacts are written under `Ignore\Benchmarks\SqlServerDataMovement\Write` and `Ignore\Benchmarks\SqlServerDataMovement\Read`, which are intentionally ignored by Git. To inspect the matrix without touching SQL Server:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Plan
```

### Write Benchmark

<!-- sqlserver-data-movement-write-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | SqlServer | Result |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| 1000 rows / batch 5000 / Class | BatchSize=5000, InputKind=Class, RowCount=1000 | Core-7.6.3 | Write | 1.00x (25ms) | 6.39x (161ms) | Skipped | DbaClientX fastest |
| 1000 rows / batch 5000 / DataReader | BatchSize=5000, InputKind=DataReader, RowCount=1000 | Core-7.6.3 | Write | 1.00x (21ms) | Skipped | Skipped | DbaClientX only successful |
| 1000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=1000 | Core-7.6.3 | Write | 1.00x (20ms) | 2.02x (41ms) | Skipped | DbaClientX fastest |
| 1000 rows / batch 5000 / PSCustomObject | BatchSize=5000, InputKind=PSCustomObject, RowCount=1000 | Core-7.6.3 | Write | 1.00x (27ms) | 4.15x (111ms) | Skipped | DbaClientX fastest |
| 100000 rows / batch 5000 / Class | BatchSize=5000, InputKind=Class, RowCount=100000 | Core-7.6.3 | Write | 1.00x (3.07s) | 4.34x (13.30s) | Skipped | DbaClientX fastest |
| 100000 rows / batch 5000 / DataReader | BatchSize=5000, InputKind=DataReader, RowCount=100000 | Core-7.6.3 | Write | 1.00x (112ms) | Skipped | Skipped | DbaClientX only successful |
| 100000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=100000 | Core-7.6.3 | Write | 1.00x (134ms) | 1.31x (175ms) | Skipped | DbaClientX fastest |
| 100000 rows / batch 5000 / PSCustomObject | BatchSize=5000, InputKind=PSCustomObject, RowCount=100000 | Core-7.6.3 | Write | 1.00x (2.82s) | 4.30x (12.16s) | Skipped | DbaClientX fastest |
| 20000 rows / batch 5000 / Class | BatchSize=5000, InputKind=Class, RowCount=20000 | Core-7.6.3 | Write | 1.00x (311ms) | 6.50x (2.02s) | Skipped | DbaClientX fastest |
| 20000 rows / batch 5000 / DataReader | BatchSize=5000, InputKind=DataReader, RowCount=20000 | Core-7.6.3 | Write | 1.00x (36ms) | Skipped | Skipped | DbaClientX only successful |
| 20000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=20000 | Core-7.6.3 | Write | 1.00x (37ms) | 1.48x (54ms) | Skipped | DbaClientX fastest |
| 20000 rows / batch 5000 / PSCustomObject | BatchSize=5000, InputKind=PSCustomObject, RowCount=20000 | Core-7.6.3 | Write | 1.00x (423ms) | 5.00x (2.11s) | Skipped | DbaClientX fastest |
| 5000 rows / batch 5000 / Class | BatchSize=5000, InputKind=Class, RowCount=5000 | Core-7.6.3 | Write | 1.00x (169ms) | 3.93x (665ms) | Skipped | DbaClientX fastest |
| 5000 rows / batch 5000 / DataReader | BatchSize=5000, InputKind=DataReader, RowCount=5000 | Core-7.6.3 | Write | 1.00x (22ms) | Skipped | Skipped | DbaClientX only successful |
| 5000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=5000 | Core-7.6.3 | Write | 1.00x (25ms) | 1.78x (44ms) | Skipped | DbaClientX fastest |
| 5000 rows / batch 5000 / PSCustomObject | BatchSize=5000, InputKind=PSCustomObject, RowCount=5000 | Core-7.6.3 | Write | 1.00x (114ms) | 4.30x (492ms) | Skipped | DbaClientX fastest |
<!-- sqlserver-data-movement-write-benchmark:end -->

### Read Benchmark

<!-- sqlserver-data-movement-read-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | SqlServer | Result |
| --- | --- | --- | --- | ---: | ---: | ---: | --- |
| 1000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=1000 | Core-7.6.3 | Read | 1.00x (8ms) | 3.45x (26ms) | Skipped | DbaClientX fastest |
| 1000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=1000 | Core-7.6.3 | Read | 1.00x (11ms) | 2.19x (23ms) | Skipped | DbaClientX fastest |
| 100000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=100000 | Core-7.6.3 | Read | 1.00x (71ms) | 1.13x (80ms) | Skipped | DbaClientX fastest |
| 100000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=100000 | Core-7.6.3 | Read | 1.00x (898ms) | 1.68x (1.51s) | Skipped | DbaClientX fastest |
| 20000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=20000 | Core-7.6.3 | Read | 1.00x (21ms) | 1.70x (36ms) | Skipped | DbaClientX fastest |
| 20000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=20000 | Core-7.6.3 | Read | 1.00x (105ms) | 1.79x (187ms) | Skipped | DbaClientX fastest |
| 5000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=5000 | Core-7.6.3 | Read | 1.00x (10ms) | 1.86x (19ms) | Skipped | DbaClientX fastest |
| 5000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=5000 | Core-7.6.3 | Read | 1.00x (17ms) | 2.90x (51ms) | Skipped | DbaClientX fastest |
<!-- sqlserver-data-movement-read-benchmark:end -->

Treat benchmark numbers as workstation evidence, not universal rankings. SQL Server version, storage, TLS, table indexes, triggers, recovery model, batch size, and client runtime can dominate the result; rerun the suite in the environment that matters.

## SQL Server CSV Export Benchmark

Use the CSV export benchmark when you want the ARPE-style export-only view: SQL Server rows out to a CSV file, without importing the file back into a table. This is intentionally separate from the round-trip benchmark, because native `bcp` is an export/import utility and does not measure the same PowerShell object or `DataTable` handoff work.

```powershell
.\Module\Examples\Benchmark.SqlServerCsvExport.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 100000 `
    -Engine DbaClientX,DbaClientXReader,DbaClientXStream,dbatools,bcp,FastBCP `
    -Iterations 3 `
    -UpdateReadme
```

The DbaClientX lane reads with `Invoke-DbaXQuery -ReturnType DataTable` and writes with PSWriteOffice `Export-OfficeCsv -NoHeader`. `DbaClientXReader` opens a DbaClientX-owned SQL Server `IDataReader` and writes it directly with `Export-OfficeCsv`, avoiding `DataTable` and `DataRow` materialization. `DbaClientXStream` uses the existing public stream shape, `Invoke-DbaXQuery -Stream -ReturnType DataRow`, then writes the streamed rows with `Export-OfficeCsv`. The dbatools lane uses `Export-DbaCsv`. The native lane uses `bcp queryout` with character mode, comma field terminators, LF row terminators, UTF-8 code page, and trusted authentication. The optional FastBCP lane uses the documented SQL Server table export shape with `--connectiontype mssql`, trusted authentication, UTF-8 CSV output, `--parallelmethod Ntile`, `--distributekeycolumn Id`, `--paralleldegree -2`, and split-file output. Successful lanes validate data-row count plus `Id` min/max/sum integrity and file size before cleanup.

Inspect the matrix without touching SQL Server:

```powershell
.\Module\Examples\Benchmark.SqlServerCsvExport.ps1 -Plan
```

The benchmark skips unavailable optional engines. `bcp` must be on `PATH`, dbatools must expose `Connect-DbaInstance` and `Export-DbaCsv`, and the DbaClientX lanes require PSWriteOffice with `Export-OfficeCsv`. FastBCP must be on `PATH`, passed through `-FastBcpPath`, or provided through `$env:FASTBCP_PATH`; use `-FastBcpParallelMethod` and `-FastBcpParallelDegree` to change the FastBCP partitioning mode.

The ARPE/FastBCP comparison is useful because it separates export-only throughput from a full file/database round trip. This benchmark covers the same local SQL Server to CSV shape for DbaClientX + PSWriteOffice, dbatools `Export-DbaCsv`, native `bcp queryout`, and FastBCP when the executable is locally available. The current snapshot shows the direct DbaClientX reader path ahead of dbatools and `bcp`; the older `DataTable` and `DataRow` lanes remain slower because they measure materialization and PowerShell row-shaping overhead in addition to file writing. FastBCP cloud-file targets and Parquet/JSON output remain product-scope gaps for this CSV/Excel benchmark story.

<!-- sqlserver-csv-export-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientXReader | bcp | DbaClientX | DbaClientXStream | dbatools | FastBCP | Result |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100000 rows / CSV export | RowCount=100000 | Core-7.6.3 | Export | 1.00x (65ms) | 4.17x (273ms) | 6.69x (437ms) | 5.25x (343ms) | 1.58x (103ms) | Skipped | DbaClientXReader fastest |
<!-- sqlserver-csv-export-benchmark:end -->

## Office File Round-Trip Benchmark

Use the office file round-trip benchmark when you want evidence for the combined DbaClientX and PSWriteOffice workflow. The measured operation reads source rows from SQL Server with DbaClientX, writes a CSV or Excel file with PSWriteOffice, imports the file back as a `DataTable`, then writes the rows to SQL Server with `Write-DbaXTableData`. Validation checks destination row count and simple integrity metrics before cleanup.

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000,5000 `
    -FileKind Csv,CsvGZip,Excel `
    -Iterations 3
```

Use the apples-to-apples CSV comparison when checking the same kind of lane dbatools publishes for its CSV library:

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 100000 `
    -FileKind Csv,CsvGZip `
    -Engine DbaClientX,dbatools `
    -Iterations 3
```

The default engine is `DbaClientX`. Add `-Engine DbaClientX,dbatools -FileKind Csv,CsvGZip` to compare the DbaClientX + PSWriteOffice CSV round trip with dbatools' CSV fast path (`Export-DbaCsv` plus `Import-DbaCsv` into SQL Server bulk copy). `CsvGZip` uses a `.csv.gz` file so both sides exercise compressed CSV. The dbatools engine is CSV-only; Excel remains a DbaClientX + PSWriteOffice lane because dbatools does not own Excel import/export.

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000,5000 `
    -FileKind Csv,CsvGZip `
    -Engine DbaClientX,dbatools `
    -Iterations 3
```

By default the wrapper imports installed `DbaClientX` and `PSWriteOffice` modules. Use `-ModulePath`, `$env:DBACLIENTX_BENCHMARK_MODULE_PATH`, `-PSWriteOfficeModulePath`, or `$env:PSWRITEOFFICE_BENCHMARK_MODULE_PATH` when benchmarking local source builds. Single-engine runs produce validated JSON/CSV/Markdown artifacts; README comparison tables are generated for engine comparisons such as `DbaClientX` versus `dbatools`.

To inspect the matrix without touching SQL Server:

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 -Plan
```

If the imported PSWriteOffice module does not expose `Export-OfficeCsv` and `Import-OfficeCsv -AsDataTable`, the wrapper skips the DbaClientX CSV lane and runs any remaining file kinds. The `CsvGZip` lane also requires PSWriteOffice CSV compression parameters so the benchmark cannot accidentally measure an uncompressed `.csv.gz` file. Use `-FileKind Excel` for installed PSWriteOffice versions without those CSV cmdlets. If dbatools is not installed or does not expose `Export-DbaCsv` and `Import-DbaCsv`, the wrapper skips the dbatools CSV lane. The compressed dbatools lane also requires `Export-DbaCsv -CompressionType`, and passes `GZip` explicitly when it is available.

The benchmark is intentionally paired with feature coverage, because the useful target is not only the fastest happy path. dbatools documents a broad CSV-to-SQL surface in `Import-DbaCsv` and `Export-DbaCsv`; this table keeps the comparable DbaClientX + PSWriteOffice surface visible while the remaining gaps are closed in the owning libraries.

| Capability | dbatools CSV path | DbaClientX + PSWriteOffice path | Benchmark visibility |
| --- | --- | --- | --- |
| SQL query/table to CSV | `Export-DbaCsv -SqlInstance/-Database/-Query/-Table` | `SqlServer.QueryReader(...)` into `Export-OfficeCsv` for the fastest export-only lane, or `Invoke-DbaXQuery -ReturnType DataTable` for the simple buffered lane | `Csv` round trip compares buffered engines; export-only table compares reader, buffered, stream, dbatools, `bcp`, and optional FastBCP shapes |
| CSV to SQL bulk load | `Import-DbaCsv` uses SQL Server bulk copy | `Import-OfficeCsv -AsDataTable` then `Write-DbaXTableData -Provider SqlServer` | `Csv` round trip compares both engines |
| Compressed CSV | `Export-DbaCsv -CompressionType` and `.csv.gz` import | OfficeIMO owns compressed CSV core; PSWriteOffice exposes `Export-OfficeCsv -CompressionType` and `Import-OfficeCsv -CompressionType` in the CSV parity surface | `CsvGZip` compares both engines when the installed modules expose compression |
| Cancellation and progress | Dataplat exposes cancellation and progress callbacks in its CSV options | OfficeIMO.CSV exposes cancellation/progress options; PSWriteOffice maps Ctrl+C cancellation and `-ProgressInterval`; DbaClientX SQL Server bulk copy has `-NotifyAfter` progress | Covered by OfficeIMO.CSV and PSWriteOffice CSV tests; benchmark uses progress-free timing |
| Excel to or from SQL | Not a dbatools CSV feature | `Export-OfficeExcel` / `Import-OfficeExcel -AsDataTable` with `Write-DbaXTableData` | `Excel` round trip covers the direct Excel path |
| Bulk-write knobs | `BatchSize`, `TableLock`, `CheckConstraints`, `FireTriggers`, `KeepIdentity`, `KeepNulls` | `Write-DbaXTableData` exposes the same SQL Server bulk-copy knobs | Covered by data-movement and office round-trip suites |
| Auto-create destination table | `Import-DbaCsv -AutoCreateTable` with optional column optimization | `Write-DbaXTableData -AutoCreateTable`; typed columns depend on the incoming `DataTable` | Covered by round-trip suites |
| Column mapping and selected columns | `Column`, `ColumnMap`, ordinal fallback | `Write-DbaXTableData -ColumnMap`; select or shape columns before the file/database boundary | Covered by command tests, not yet by office round-trip benchmark |
| CSV dialect basics | Delimiter, no header, quote, encoding, null value, date format, UTC conversion, culture-oriented parsing | Delimiter, no header/header, quote mode, encoding, null value, date format, UTC conversion, culture, trimming, comments, W3C headers | Covered by PSWriteOffice CSV tests; round-trip benchmark uses the default dialect |
| Messy CSV handling | Skip rows, comments, duplicate header behavior, mismatched rows, quote mode, static columns, parse-error collection | Skip rows, comments, W3C headers, duplicate header behavior, column-count mismatch policy, strict/lenient quote parsing, static columns, parse-error collection/skip-row, field-length limits, quote normalization, string interning | Mostly covered by PSWriteOffice CSV tests; round-trip benchmark keeps the default happy path |
| Security limits | Max field length and decompression protection | OfficeIMO.CSV owns `MaxFieldLength` and `MaxDecompressedBytes`; PSWriteOffice exposes both on file-reading CSV cmdlets | Covered by OfficeIMO.CSV and PSWriteOffice CSV tests; not timed by default |
| Multi-character delimiters | Supported by the Dataplat/dbatools CSV library | OfficeIMO.CSV supports `DelimiterText`; PSWriteOffice exposes `-DelimiterText` on CSV read/write surfaces | Covered by OfficeIMO.CSV and PSWriteOffice CSV tests; not part of the default SQL round-trip timing lane |
| Type detection and custom conversion for CSV import | `SampleRows`, `DetectColumnTypes`, and custom converters can drive SQL column creation | OfficeIMO.CSV has schema inference, built-in typed conversion, and schema-level custom converters in the C# core; PSWriteOffice exposes `Import-OfficeCsv -AsDataTable -InferSchema` before `Write-DbaXTableData -AutoCreateTable` | Covered by OfficeIMO.CSV and PSWriteOffice CSV tests; not yet a separate SQL benchmark lane |
| Parallel CSV import | `Parallel`, `ThrottleLimit`, `ParallelBatchSize` | Database/provider parallel query helpers exist, but CSV-to-SQL parallel import is not a current round-trip feature | Not benchmarked yet |
| Parallel partitioned CSV export | FastBCP-style split export can partition source data and write multiple files | DbaClientX + PSWriteOffice writes one local CSV file; the optional FastBCP benchmark lane measures split-file partitioned export when FastBCP is available | Covered as a separate optional engine; do not treat single-file DbaClientX results as a parallel-export comparison |
| Direct cloud/object-storage output | FastBCP publishes local and cloud target support | Current DbaClientX + PSWriteOffice examples write local files; upload/sync belongs in a storage owner above the CSV writer | Documented gap; not benchmarked |
| Parquet/JSON export | FastBCP positions Parquet/JSON as additional output formats | PSWriteOffice owns CSV/Excel here; DbaClientX stays database/provider focused | Out of scope for CSV/Excel benchmark story |
| Cross-platform module use | dbatools export is commonly Windows-first in the ARPE table | DbaClientX provider libraries target Windows and Linux/macOS on modern .NET; the PowerShell module targets Windows PowerShell/.NET Framework on Windows and PowerShell 7/.NET on all platforms | Platform matrix below; benchmark engines still depend on locally available tools |

### Benchmark Coverage Map

The comparison is split by ownership so the benchmark does not cherry-pick only our strongest lanes or mix unlike work into one table.

| Competitor lane | DbaClientX / OfficeIMO lane | Where to run it | Current README evidence |
| --- | --- | --- | --- |
| Native `bcp queryout` SQL-to-CSV export | DbaClientX reader/DataTable/DataRow SQL read plus PSWriteOffice CSV write | `Benchmark.SqlServerCsvExport.ps1 -Engine DbaClientXReader,DbaClientX,DbaClientXStream,bcp` | `sqlserver-csv-export-benchmark` |
| dbatools `Export-DbaCsv` SQL-to-CSV export | DbaClientX reader/DataTable/DataRow SQL read plus PSWriteOffice CSV write | `Benchmark.SqlServerCsvExport.ps1 -Engine DbaClientXReader,DbaClientX,DbaClientXStream,dbatools` | `sqlserver-csv-export-benchmark` |
| ARPE/FastBCP single-purpose SQL-to-CSV export | DbaClientX reader/DataTable/DataRow SQL read plus PSWriteOffice CSV write | `Benchmark.SqlServerCsvExport.ps1 -Engine DbaClientXReader,DbaClientX,DbaClientXStream,FastBCP` | FastBCP lane is skipped unless the executable is available |
| ARPE/FastBCP parallel partitioned export | Optional FastBCP split-file export with `Ntile` and `Id` distribution | `Benchmark.SqlServerCsvExport.ps1 -Engine FastBCP -FastBcpParallelMethod Ntile` | FastBCP lane is skipped unless the executable is available |
| dbatools `Export-DbaCsv` plus `Import-DbaCsv` SQL round trip | DbaClientX SQL read/write plus PSWriteOffice CSV | `Benchmark.OfficeFileRoundTrip.ps1 -Engine DbaClientX,dbatools -FileKind Csv` | `office-file-roundtrip-benchmark` |
| dbatools compressed CSV round trip | DbaClientX SQL read/write plus PSWriteOffice `.csv.gz` | `Benchmark.OfficeFileRoundTrip.ps1 -Engine DbaClientX,dbatools -FileKind CsvGZip` | `office-file-roundtrip-benchmark` |
| dbatools `Write-DbaDbTableData` client-side import | `Write-DbaXTableData` from `DataTable`, objects, classes, and `IDataReader` | `Benchmark.SqlServerDataMovement.ps1 -Operation Write` | `sqlserver-data-movement-write-benchmark` |
| dbatools `Invoke-DbaQuery` materialization | `Invoke-DbaXQuery` to `DataTable` or PowerShell objects | `Benchmark.SqlServerDataMovement.ps1 -Operation Read` | `sqlserver-data-movement-read-benchmark` |
| Dataplat/dbatools raw parser small, medium, large, wide, quoted, modern Sep/Sylvan/CsvHelper, all-values, and quick-test single/all-column lanes | OfficeIMO.CSV raw parser and reader bridge | OfficeIMO.CSV benchmark suite, including the dbatools-library parity benchmark | Tracked in OfficeIMO.CSV, not duplicated in DbaClientX |
| Excel to or from SQL | DbaClientX SQL read/write plus PSWriteOffice Excel | `Benchmark.OfficeFileRoundTrip.ps1 -FileKind Excel -Engine DbaClientX` | Validated as a DbaClientX + PSWriteOffice lane; dbatools CSV has no equivalent Excel lane |
| CSV type-converter/vector microbenchmarks | Future typed schema/import work, if it becomes part of the file/database contract | Not currently a DbaClientX SQL round-trip lane | Documented as out of scope for this suite |

<!-- office-file-roundtrip-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | Result |
| --- | --- | --- | --- | ---: | ---: | --- |
| 100000 rows / Csv | FileKind=Csv, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (1.38s) | 2.18x (3.02s) | DbaClientX fastest |
| 100000 rows / CsvGZip | FileKind=CsvGZip, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (1.35s) | 2.27x (3.06s) | DbaClientX fastest |
<!-- office-file-roundtrip-benchmark:end -->

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
    serverOrInstance: "SQL1",
    database: "App",
    integratedSecurity: true,
    table: table,
    destinationTable: "dbo.ImportUsers",
    options: new SqlServerBulkInsertOptions
    {
        AutoCreateTable = true,
        BulkCopyOptions = Microsoft.Data.SqlClient.SqlBulkCopyOptions.TableLock
    },
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
| Benchmarks | Current PowerShell host through PSPublishModule | Current PowerShell host through PSPublishModule |

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
| [`Build`](Build) | Project release configuration |

## Examples

- PowerShell examples: [`Module/Examples`](Module/Examples)
- C# examples: [`DbaClientX.Examples`](DbaClientX.Examples)
- Benchmarks: [`Module/Examples/Benchmark.SqlServerDataMovement.ps1`](Module/Examples/Benchmark.SqlServerDataMovement.ps1), [`Module/Examples/Benchmark.SqlServerCsvExport.ps1`](Module/Examples/Benchmark.SqlServerCsvExport.ps1), [`Module/Examples/Benchmark.OfficeFileRoundTrip.ps1`](Module/Examples/Benchmark.OfficeFileRoundTrip.ps1)
- Data movement guide: [`docs/data-movement.md`](docs/data-movement.md)
- SQL Server benchmark notes: [`docs/sqlserver-benchmark-notes.md`](docs/sqlserver-benchmark-notes.md)

Useful example files:

- [`Example.QuerySqlServer.ps1`](Module/Examples/Example.QuerySqlServer.ps1)
- [`Example.SqlServerDataMovement.ps1`](Module/Examples/Example.SqlServerDataMovement.ps1)
- [`Example.CsvRoundTrip.ps1`](Module/Examples/Example.CsvRoundTrip.ps1)
- [`Example.ExcelRoundTrip.ps1`](Module/Examples/Example.ExcelRoundTrip.ps1)
- [`Benchmark.SqlServerDataMovement.ps1`](Module/Examples/Benchmark.SqlServerDataMovement.ps1)
- [`Benchmark.SqlServerCsvExport.ps1`](Module/Examples/Benchmark.SqlServerCsvExport.ps1)
- [`Benchmark.OfficeFileRoundTrip.ps1`](Module/Examples/Benchmark.OfficeFileRoundTrip.ps1)
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
