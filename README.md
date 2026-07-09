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

DbaClientX provides provider connections, query execution, transactions, metadata, and bulk-copy commands for scripts and .NET code. It works with normal tabular inputs such as `DataTable`, `DataView`, `IDataReader`, `DataRow`, hashtables, regular objects, and rows imported from CSV or Excel with PSWriteOffice.

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

| You need to... | Use this | Notes |
| --- | --- | --- |
| Write PowerShell objects, `DataTable`, `DataView`, `IDataReader`, or Excel-imported rows to a table | `Write-DbaXTableData` | Uses provider-native database writes |
| Load a SQL Server staging table and let the command create the table, map columns, lock the table, preserve identities/nulls, fire triggers, check constraints, or report progress | `Write-DbaXTableData -Provider SqlServer` | SQL Server-specific knobs map to `SqlServerBulkInsertOptions` |
| Copy one or more tables between database providers | `Copy-DbaXTableData` | Uses reusable copy definitions plus provider adapters |
| Export SQL rows to CSV, compressed CSV, or Excel | `SqlServer.QueryReader(...)` or `Invoke-DbaXQuery -ReturnType DataTable` plus PSWriteOffice `Export-OfficeCsv` / `Export-OfficeExcel` | Streams or buffers database rows into the file writer |
| Import CSV, compressed CSV, or Excel into SQL Server | PSWriteOffice `Import-OfficeCsv -AsDataReader` / `Import-OfficeExcel -AsDataReader` plus `Write-DbaXTableData` | Reads the file as tabular data, then bulk-writes it |
| Stream a reader into SQL Server bulk copy | `Write-DbaXTableData -Provider SqlServer -InputObject (, $reader)` | Pass the reader as a single input object with `, $reader` |

CSV and Excel round trips use matching DbaClientX, PSWriteOffice, and OfficeIMO
packages. Use `-PSWriteOfficeModulePath` only when validating a local
PSWriteOffice build.

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

Import CSV with PSWriteOffice, then hand the reader to DbaClientX for the SQL Server write:

```powershell
$reader = $null
try {
    $reader = Import-OfficeCsv .\Users.csv -AsDataReader -InferSchema
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsers' `
        -InputObject (, $reader) `
        -AutoCreateTable `
        -BatchSize 5000
} finally {
    if ($null -ne $reader) {
        $reader.Dispose()
    }
}
```

For Excel, or for providers that prefer a fully materialized table, use the `DataTable` output shape.

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

Load compressed CSV back into SQL Server with the same streaming table-write command:

```powershell
$reader = $null
try {
    $reader = Import-OfficeCsv .\Users.csv.gz -CompressionType GZip -AsDataReader -InferSchema
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsers' `
        -InputObject (, $reader) `
        -AutoCreateTable `
        -BatchSize 5000
} finally {
    if ($null -ne $reader) {
        $reader.Dispose()
    }
}
```

The same database write path accepts Excel-imported readers:

```powershell
$reader = Import-OfficeExcel .\Users.xlsx -AsDataReader
try {
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=.;Database=App;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'dbo.ImportUsers' `
        -InputObject (, $reader) `
        -AutoCreateTable `
        -TableLock
} finally {
    $reader.Dispose()
}
```

Run the full SQL Server -> file -> SQL Server examples when you want to prove both sides together:

```powershell
.\Module\Examples\Example.CsvRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts
.\Module\Examples\Example.ExcelRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts
```

The examples create SQL Server source rows, export them to a `.csv` or `.xlsx` file with PSWriteOffice, import the file back as a streaming reader, write to SQL Server with `-AutoCreateTable`, and fail if any row count does not match.

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

## SQL Server Benchmarks

Current workstation timings are below. Commands, measured operations, validation,
and artifact details are in [SQL Server benchmark notes](docs/sqlserver-benchmark-notes.md).

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

## SQL Server CSV Export

<!-- sqlserver-csv-export-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientXReader | bcp | DbaClientX | DbaClientXStream | dbatools | FastBCP | Result |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 100000 rows / CSV export | RowCount=100000 | Core-7.6.3 | Export | 1.00x (65ms) | 4.17x (273ms) | 6.69x (437ms) | 5.25x (343ms) | 1.58x (103ms) | Skipped | DbaClientXReader fastest |
<!-- sqlserver-csv-export-benchmark:end -->

## Office File Round Trip

<!-- office-file-roundtrip-benchmark:start -->
| Scenario | Variables | Host | Operation | DbaClientX | dbatools | Result |
| --- | --- | --- | --- | ---: | ---: | --- |
| 100000 rows / Csv | ColumnShape=Default, FileKind=Csv, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (639ms) | 4.07x (2.60s) | DbaClientX fastest |
| 100000 rows / CsvGZip | ColumnShape=Default, FileKind=CsvGZip, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (658ms) | 3.99x (2.63s) | DbaClientX fastest |
| 100000 rows / CsvGZipTyped | ColumnShape=Default, FileKind=CsvGZipTyped, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (561ms) | 1.10x (614ms) | DbaClientX fastest |
| 100000 rows / CsvTyped | ColumnShape=Default, FileKind=CsvTyped, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (567ms) | 1.18x (671ms) | DbaClientX fastest |
| 100000 rows / CsvTyped / Mapped columns | ColumnShape=Mapped, FileKind=CsvTyped, RowCount=100000 | Core-7.6.3 | RoundTrip | 1.00x (282ms) | 1.92x (541ms) | DbaClientX fastest |
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
