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
| Export SQL rows to CSV or Excel and import the file back | DbaClientX cmdlets plus PSWriteOffice `Export-OfficeCsv` / `Import-OfficeCsv` or `Export-OfficeExcel` / `Import-OfficeExcel` | DbaClientX owns database work; PSWriteOffice/OfficeIMO owns file-format work |

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

Use `Write-DbaXTableData` when the data is already in PowerShell and the next step is a database table. The command accepts `DataTable`, `DataView`, `IDataReader`, `DataRow`, hashtables, and regular objects from the pipeline.

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
    -InputKind DataTable, PSCustomObject, Class `
    -Iterations 3
```

The suite benchmarks SQL Server writes and reads separately. The write suite covers DbaClientX across `DataTable`, `PSCustomObject`, and typed class input shapes, and adds dbatools and SqlServer module lanes only when `Write-DbaDbTableData` or `Write-SqlTableData` are available. The dbatools `DataTable` write lane uses a direct value passed to `-InputObject` so it stays on the documented SqlBulkCopy fast path instead of the slower piped-`DataRow` path. `Copy-DbaDbTableData` is intentionally not part of this matrix because it measures SQL table-to-table streaming rather than client-side object/DataTable import.

The read suite seeds an isolated SQL Server table outside the measured operation, then compares DbaClientX `Invoke-DbaXQuery` with dbatools `Invoke-DbaQuery` when dbatools is available. By default it reads every row as both `DataTable` and PowerShell-object output; `DataSet` can be selected with `-ReadShape DataSetAll` for local diagnosis. Successful lanes verify row counts plus simple data integrity (`Id` min/max/sum and `Score` sum) before dropping their isolated tables; failed lanes leave their table behind for inspection.

By default the wrapper imports the installed `DbaClientX` module. Use `-ModulePath`, `$env:DBACLIENTX_BENCHMARK_MODULE_PATH`, or `$env:DBACLIENTX_DEVELOPMENT_PATH` when benchmarking a local source build.

The checked-in snapshot below uses `DataTable` write input and full-result `DataTable` plus `PSObject` reads at 1k, 5k, 20k, and 100k rows. Pass additional `-InputKind` or `-ReadShape` values for a broader local matrix.

The suite rewrites the marker-delimited tables below when it runs from a source checkout. Artifacts are written under `Ignore\Benchmarks\SqlServerDataMovement\Write` and `Ignore\Benchmarks\SqlServerDataMovement\Read`, which are intentionally ignored by Git. To inspect the matrix without touching SQL Server:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Plan
```

### Write Benchmark

<!-- sqlserver-data-movement-write-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | Result |
| --- | --- | --- | --- | ---: | ---: | --- |
| 1000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=1000 | Core-7.6.3 | Write | 1.00x (112ms) | 26.47x (2.97s) | DbaClientX fastest |
| 5000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=5000 | Core-7.6.3 | Write | 1.00x (41ms) | 267.14x (11.06s) | DbaClientX fastest |
| 20000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=20000 | Core-7.6.3 | Write | 1.00x (116ms) | 497.23x (57.51s) | DbaClientX fastest |
| 100000 rows / batch 5000 / DataTable | BatchSize=5000, InputKind=DataTable, RowCount=100000 | Core-7.6.3 | Write | 1.00x (279ms) | 1055.78x (294.59s) | DbaClientX fastest |
<!-- sqlserver-data-movement-write-benchmark:end -->

### Read Benchmark

<!-- sqlserver-data-movement-read-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | Result |
| --- | --- | --- | --- | ---: | ---: | --- |
| 1000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=1000 | Core-7.6.3 | Read | 1.00x (8ms) | 3.31x (25ms) | DbaClientX fastest |
| 1000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=1000 | Core-7.6.3 | Read | 1.00x (7ms) | 4.10x (30ms) | DbaClientX fastest |
| 5000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=5000 | Core-7.6.3 | Read | 1.00x (9ms) | 1.96x (18ms) | DbaClientX fastest |
| 5000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=5000 | Core-7.6.3 | Read | 1.00x (16ms) | 5.36x (87ms) | DbaClientX fastest |
| 20000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=20000 | Core-7.6.3 | Read | 1.00x (29ms) | 1.46x (42ms) | DbaClientX fastest |
| 20000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=20000 | Core-7.6.3 | Read | 1.00x (95ms) | 2.50x (236ms) | DbaClientX fastest |
| 100000 rows / DataTableAll | ReadShape=DataTableAll, RowCount=100000 | Core-7.6.3 | Read | 1.00x (147ms) | 1.08x (159ms) | DbaClientX fastest |
| 100000 rows / PSObjectAll | ReadShape=PSObjectAll, RowCount=100000 | Core-7.6.3 | Read | 1.00x (1.49s) | 1.56x (2.33s) | DbaClientX fastest |
<!-- sqlserver-data-movement-read-benchmark:end -->

Treat benchmark numbers as workstation evidence, not universal rankings. SQL Server version, storage, TLS, table indexes, triggers, recovery model, batch size, and client runtime can dominate the result; rerun the suite in the environment that matters.

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

By default the wrapper imports installed `DbaClientX` and `PSWriteOffice` modules. Use `-ModulePath`, `$env:DBACLIENTX_BENCHMARK_MODULE_PATH`, `-PSWriteOfficeModulePath`, or `$env:PSWRITEOFFICE_BENCHMARK_MODULE_PATH` when benchmarking local source builds. To inspect the matrix without touching SQL Server:

If the imported PSWriteOffice module does not expose `Export-OfficeCsv` and `Import-OfficeCsv -AsDataTable`, the wrapper skips the DbaClientX CSV lane and runs any remaining file kinds. The `CsvGZip` lane also requires PSWriteOffice CSV compression parameters so the benchmark cannot accidentally measure an uncompressed `.csv.gz` file. Use `-FileKind Excel` for installed PSWriteOffice versions without those CSV cmdlets. If dbatools is not installed or does not expose `Export-DbaCsv` and `Import-DbaCsv`, the wrapper skips the dbatools CSV lane. The compressed dbatools lane also requires `Export-DbaCsv -CompressionType`, and passes `GZip` explicitly when it is available.

The benchmark is intentionally paired with feature coverage, because the useful target is not only the fastest happy path. dbatools documents a broad CSV-to-SQL surface in `Import-DbaCsv` and `Export-DbaCsv`; this table keeps the comparable DbaClientX + PSWriteOffice surface visible while the remaining gaps are closed in the owning libraries.

| Capability | dbatools CSV path | DbaClientX + PSWriteOffice path | Benchmark visibility |
| --- | --- | --- | --- |
| SQL query/table to CSV | `Export-DbaCsv -SqlInstance/-Database/-Query/-Table` | `Invoke-DbaXQuery -ReturnType DataTable` then `Export-OfficeCsv` | `Csv` round trip compares both engines |
| CSV to SQL bulk load | `Import-DbaCsv` uses SQL Server bulk copy | `Import-OfficeCsv -AsDataTable` then `Write-DbaXTableData -Provider SqlServer` | `Csv` round trip compares both engines |
| Compressed CSV | `Export-DbaCsv -CompressionType` and `.csv.gz` import | OfficeIMO owns compressed CSV core; PSWriteOffice exposes `Export-OfficeCsv -CompressionType` and `Import-OfficeCsv -CompressionType` in the CSV parity surface | `CsvGZip` compares both engines when the installed modules expose compression |
| Excel to or from SQL | Not a dbatools CSV feature | `Export-OfficeExcel` / `Import-OfficeExcel -AsDataTable` with `Write-DbaXTableData` | `Excel` round trip covers the direct Excel path |
| Bulk-write knobs | `BatchSize`, `TableLock`, `CheckConstraints`, `FireTriggers`, `KeepIdentity`, `KeepNulls` | `Write-DbaXTableData` exposes the same SQL Server bulk-copy knobs | Covered by data-movement and office round-trip suites |
| Auto-create destination table | `Import-DbaCsv -AutoCreateTable` with optional column optimization | `Write-DbaXTableData -AutoCreateTable`; typed columns depend on the incoming `DataTable` | Covered by round-trip suites |
| Column mapping and selected columns | `Column`, `ColumnMap`, ordinal fallback | `Write-DbaXTableData -ColumnMap`; select or shape columns before the file/database boundary | Covered by command tests, not yet by office round-trip benchmark |
| CSV dialect basics | Delimiter, no header, quote, encoding, null value, date format, UTC conversion, culture-oriented parsing | Delimiter, no header/header, quote mode, encoding, null value, date format, UTC conversion, culture, trimming, comments, W3C headers | Covered by PSWriteOffice CSV tests; round-trip benchmark uses the default dialect |
| Messy CSV handling | Skip rows, comments, duplicate header behavior, mismatched rows, quote mode, static columns, parse-error collection | Skip rows, comments, W3C headers, duplicate header behavior, column-count mismatch policy, strict/lenient quote parsing, static columns; parse-error collection remains a gap | Mostly covered by PSWriteOffice CSV tests; round-trip benchmark keeps the default happy path |
| Multi-character delimiters | Supported by the Dataplat/dbatools CSV library | OfficeIMO.CSV and PSWriteOffice currently support single-character delimiters only | Documented boundary; not benchmarked as an equivalent lane |
| Type detection for CSV import | `SampleRows` and `DetectColumnTypes` can drive SQL column creation | OfficeIMO.CSV has schema inference in the C# core; PSWriteOffice `-AsDataTable` remains string/DataTable-oriented before `Write-DbaXTableData -AutoCreateTable` | Not benchmarked yet as a SQL type-inference lane |
| Parallel CSV import | `Parallel`, `ThrottleLimit`, `ParallelBatchSize` | Database/provider parallel query helpers exist, but CSV-to-SQL parallel import is not a current round-trip feature | Not benchmarked yet |

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 -Plan
```

<!-- office-file-roundtrip-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | Result |
| --- | --- | --- | --- | ---: | ---: | --- |
| 1000 rows / Csv | FileKind=Csv, RowCount=1000 | Core-7.6.3 | RoundTrip | 1.00x (152ms) | 2.67x (405ms) | DbaClientX fastest |
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
- Benchmarks: [`Module/Examples/Benchmark.SqlServerDataMovement.ps1`](Module/Examples/Benchmark.SqlServerDataMovement.ps1)
- Data movement guide: [`docs/data-movement.md`](docs/data-movement.md)
- SQL Server benchmark notes: [`docs/sqlserver-benchmark-notes.md`](docs/sqlserver-benchmark-notes.md)

Useful example files:

- [`Example.QuerySqlServer.ps1`](Module/Examples/Example.QuerySqlServer.ps1)
- [`Example.SqlServerDataMovement.ps1`](Module/Examples/Example.SqlServerDataMovement.ps1)
- [`Example.ExcelRoundTrip.ps1`](Module/Examples/Example.ExcelRoundTrip.ps1)
- [`Benchmark.SqlServerDataMovement.ps1`](Module/Examples/Benchmark.SqlServerDataMovement.ps1)
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
