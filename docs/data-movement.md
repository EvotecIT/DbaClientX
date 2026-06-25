# DbaClientX data movement

DbaClientX is the reusable database access layer for .NET and PowerShell code that needs to query, stage, or bulk-load data. Keep higher-level tools focused on their domain work, then hand tabular data to DbaClientX for provider-specific writes.

Common flows:

- SQL Server staging tables for reports, inventories, or synchronization jobs.
- PSWriteOffice import/export jobs that read Excel into `DataTable` objects and bulk-write them to a database.
- Cross-provider scripts where the same PowerShell shape writes to SQL Server, PostgreSQL, MySQL, SQLite, or Oracle.
- .NET services that should reference provider packages through DbaClientX instead of repeating provider-specific connection and bulk-copy code.

## SQL Server smoke test

Use `tempdb` first when validating a workstation or runner. The example creates a unique table, writes regular PowerShell objects through `Write-DbaXTableData`, verifies the row count, and drops the table.

```powershell
.\Module\Examples\Example.SqlServerDataMovement.ps1 -Server localhost -RowCount 100
```

For an installed module:

```powershell
Import-Module DbaClientX

$server = 'localhost'
$database = 'tempdb'
$table = 'dbo.DbaClientXSmoke'
$connectionString = "Server=$server;Database=$database;Encrypt=True;TrustServerCertificate=True;Integrated Security=True"

Invoke-DbaXNonQuery -Server $server -Database $database -TrustServerCertificate -Query @"
IF OBJECT_ID(N'$table', N'U') IS NOT NULL DROP TABLE $table;
CREATE TABLE $table
(
    Id int NOT NULL,
    DisplayName nvarchar(100) NOT NULL,
    Score decimal(18,2) NOT NULL,
    CreatedUtc datetime2 NOT NULL
);
"@

try {
    $rows = 1..100 | ForEach-Object {
        [pscustomobject]@{
            Id = $_
            DisplayName = "Row $_"
            Score = [decimal]($_ * 1.25)
            CreatedUtc = [datetime]::UtcNow
        }
    }

    $rows | Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString $connectionString `
        -DestinationTable $table `
        -BatchSize 25 `
        -PassThru

    Invoke-DbaXQuery `
        -Server $server `
        -Database $database `
        -TrustServerCertificate `
        -Query "SELECT COUNT(*) AS [RowsLoaded] FROM $table;" `
        -ReturnType PSObject
}
finally {
    Invoke-DbaXNonQuery -Server $server -Database $database -TrustServerCertificate -Query "DROP TABLE IF EXISTS $table;"
}
```

## Moving data from files

Prefer a real tabular shape at the boundary. `Write-DbaXTableData` accepts `DataTable`, `DataView`, `IDataReader`, `DataRow`, hashtables, and regular objects.

```powershell
Import-OfficeExcel .\Customers.xlsx -AsDataTable |
    Write-DbaXTableData `
        -Provider SqlServer `
        -ConnectionString 'Server=sql01;Database=warehouse;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
        -DestinationTable 'staging.Customers' `
        -BatchSize 5000
```

If the upstream library is OfficeIMO or another .NET component, keep the reusable document/file-format work there, return `DataTable` or `IDataReader`, and let DbaClientX own the database write.

## SQL Server bulk options

SQL Server bulk loads can use the same thin DbaClientX path while opting into provider-specific behavior when the destination requires it:

```powershell
Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=sql01;Database=warehouse;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'staging.Customers' `
    -InputObject $customerTable `
    -ColumnMap @{ CustomerName = 'DisplayName'; CustomerId = 'Id' } `
    -TableLock `
    -KeepIdentity `
    -KeepNulls `
    -NotifyAfter 5000 `
    -PassThru
```

`-ColumnMap`, `-TableLock`, `-CheckConstraints`, `-FireTriggers`, `-KeepIdentity`, `-KeepNulls`, and `-NotifyAfter` are SQL Server-specific. Other providers reject those switches instead of silently ignoring them.

## Other providers

The cmdlet stays the same; change the provider and connection string.

```powershell
$rows | Write-DbaXTableData `
    -Provider PostgreSql `
    -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret' `
    -DestinationTable 'public.import_customers' `
    -BatchSize 5000

$rows | Write-DbaXTableData `
    -Provider MySql `
    -ConnectionString 'Server=localhost;Database=app;User ID=user;Password=secret;SslMode=Required' `
    -DestinationTable 'import_customers' `
    -BatchSize 5000

$rows | Write-DbaXTableData `
    -Provider SQLite `
    -ConnectionString 'Data Source=C:\Data\app.db' `
    -DestinationTable 'import_customers'

$rows | Write-DbaXTableData `
    -Provider Oracle `
    -ConnectionString 'User Id=app;Password=secret;Data Source=localhost/XEPDB1' `
    -DestinationTable 'IMPORT_CUSTOMERS' `
    -BatchSize 5000
```

Provider-specific behavior, package versions, connection validation, retry conventions, and bulk-write implementation belong in DbaClientX. Consumer scripts should only own source data selection, destination table naming, and credentials.

## Benchmarking

Use the benchmark example to compare DbaClientX against locally installed PowerShell competitors without adding dependencies to the repo:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Server localhost -RowCount 5000 -Iterations 3
```

The script always benchmarks `Write-DbaXTableData`. If `Write-DbaDbTableData` from `dbatools` or `Write-SqlTableData` from the Microsoft `SqlServer` module is already installed, it adds those runs. Otherwise it reports only DbaClientX results.

Interpret the numbers as local evidence, not a universal ranking. SQL Server version, disk, network, TLS, table indexes, triggers, recovery model, batch size, and client runtime can dominate the result.

See [`sqlserver-benchmark-notes.md`](sqlserver-benchmark-notes.md) for local comparison evidence and dbatools behavior worth considering in future DbaClientX provider work.
