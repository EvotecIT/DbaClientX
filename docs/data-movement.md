# DbaClientX data movement

DbaClientX is the reusable database access layer for .NET and PowerShell code that needs to query, stage, or bulk-load data. Keep higher-level tools focused on their domain work, then hand tabular data to DbaClientX for provider-specific writes.

Common flows:

- SQL Server staging tables for reports, inventories, or synchronization jobs.
- PSWriteOffice import/export jobs that read Excel into `DataTable` objects and bulk-write them to a database.
- Cross-provider scripts where the same PowerShell shape writes to SQL Server, PostgreSQL, MySQL, SQLite, or Oracle.
- Table-to-table migrations between provider connections, such as SQLite history stores moving into SQL Server.
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

## Copying table data between providers

Use `Copy-DbaXTableData` when both sides are database tables and DbaClientX should own the read, page, bulk-write, progress, and row-count verification loop. This is the friendly PowerShell surface over the reusable `DBAClientX.DataMovement.DbaTableCopyEngine` in `DbaClientX.Core`.

```powershell
Copy-DbaXTableData `
    -SourceProvider SQLite `
    -SourceConnectionString 'Data Source=C:\Data\history.db' `
    -SourceTable 'ProbeResults' `
    -DestinationProvider SqlServer `
    -DestinationConnectionString 'Server=sql01;Database=monitoring;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.ProbeResults' `
    -OrderBy Id `
    -PageSize 10000 `
    -BatchSize 5000 `
    -TableLock `
    -ClearDestination `
    -PassThru
```

`-OrderBy` is required by default because paged migration should be deterministic. Use `-AllowUnordered` only for ad hoc copies where the provider's natural order is acceptable. `-ColumnMap` can rename columns while copying:

```powershell
Copy-DbaXTableData `
    -SourceProvider SqlServer `
    -SourceConnectionString $sourceConnectionString `
    -SourceTable 'staging.Users' `
    -DestinationProvider PostgreSql `
    -DestinationConnectionString $destinationConnectionString `
    -DestinationTable 'public.users' `
    -OrderBy UserId `
    -ColumnMap @{ UserId = 'id'; DisplayName = 'display_name' } `
    -PassThru
```

When providers differ in type affinity or destination schema, shape each page before the bulk write. This is useful for TestimoX-style SQLite to SQL Server history migrations where SQLite helper columns should be dropped, identity columns should be omitted, and numeric flags should become SQL Server `bit` values:

```powershell
Copy-DbaXTableData `
    -SourceProvider SQLite `
    -SourceConnectionString 'Data Source=C:\Data\monitoring.sqlite' `
    -SourceTable 'Monitoring_ProbeResults' `
    -DestinationProvider SqlServer `
    -DestinationConnectionString 'Server=sql01;Database=monitoring;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.Monitoring_ProbeResults' `
    -OrderBy ResultId `
    -ExcludeColumn '__MigrationRowId' `
    -ColumnMap @{ DisplayName = 'ProbeDisplayName' } `
    -BooleanColumn IsMaintenance `
    -Int32Column ProbeType, StatusId, LatencyMs, DurationMs `
    -BatchSize 5000 `
    -PassThru
```

Some operational stores have lookup or "latest state" tables where repeated source rows should collapse to one effective row per key before moving into the destination. Use source deduplication for that shape:

```powershell
Copy-DbaXTableData `
    -SourceProvider SQLite `
    -SourceConnectionString 'Data Source=C:\Data\monitoring.sqlite' `
    -SourceTable 'Monitoring_ProbeIndex' `
    -DestinationProvider SqlServer `
    -DestinationConnectionString 'Server=sql01;Database=monitoring;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'dbo.Monitoring_ProbeIndex' `
    -OrderBy ProbeName `
    -DeduplicateSourceBy ProbeName `
    -DeduplicateSourceOrderBy LastCompletedUtcMs `
    -DeduplicateSourceCaseInsensitive `
    -TreatMissingTablesAsEmpty `
    -PassThru
```

`-DeduplicateSourceBy` partitions the source rows by key, `-DeduplicateSourceOrderBy` picks the winning row for each key using descending order, and `-DeduplicateSourceCaseInsensitive` uses lowercase keys for providers that should merge values such as `Server1` and `server1`. `-TreatMissingTablesAsEmpty` is useful when migrating stores across schema versions where older source databases may not contain every current table.

The reusable .NET core owns the same behavior through `DbaTableCopyDefinition.ColumnMappings`, `ExcludedColumns`, `ColumnTypeConversions`, and `SourceOptions`. PowerShell only maps the friendly `-ExcludeColumn`, `-BooleanColumn`, `-Int32Column`, `-Int64Column`, `-DecimalColumn`, `-StringColumn`, `-DateTimeColumn`, and source-deduplication parameters into that definition.

The cmdlet supports SQL Server, PostgreSQL, MySQL, SQLite, and Oracle as source or destination providers. SQL Server destinations also support `-TableLock`, `-CheckConstraints`, `-FireTriggers`, `-KeepIdentity`, and `-KeepNulls`. SQLite destination connection strings are normalized with pooling disabled for short-lived file copy workflows.

For local proof without SQL Server, run:

```powershell
.\Module\Examples\Example.CopyTableData.ps1 -RowCount 100
```

For the SQLite to SQL Server migration shape used by tools such as TestimoX:

```powershell
.\Module\Examples\Example.CopyTableData.ps1 -CopyToSqlServer -Server localhost -Database tempdb -RowCount 1000
```

In .NET code, keep product-specific schema creation and table lists in the consumer, then hand the copy loop to `DbaTableCopyEngine` with provider-backed implementations of `IDbaTableCopySource` and `IDbaTableCopyDestination`. That lets consumer projects describe "what tables matter" while DbaClientX owns "how to page, write, verify, and report the movement."

For multi-table migrations, use `DbaTableCopyPlanner` to turn metadata into reusable definitions instead of rebuilding the same mapping/exclusion logic in each consumer:

```csharp
using DBAClientX.DataMovement;

var plan = DbaTableCopyPlanner.BuildPlan(
    sourceTables,
    sourceColumns,
    sourceIndexes,
    destinationColumns,
    new DbaTableCopyPlanOptions
    {
        DestinationSchema = "dbo",
        ExcludeDestinationIdentityColumns = true,
        ExcludedColumns = new[] { "__MigrationRowId", "__MigrationRank" },
        ColumnMappings = new Dictionary<string, string>
        {
            ["DisplayName"] = "ProbeDisplayName"
        },
        ColumnTypeConversions = new Dictionary<string, DbaTableCopyColumnType>
        {
            ["IsMaintenance"] = DbaTableCopyColumnType.Boolean
        },
        TableSourceOptions = new Dictionary<string, DbaTableCopySourceOptions>
        {
            ["ProbeIndex"] = new DbaTableCopySourceOptions(
                DeduplicateByColumns: new[] { "ProbeName" },
                DeduplicateOrderByColumns: new[] { "LastCompletedUtcMs" },
                DeduplicateCaseInsensitive: true)
        }
    });

if (plan.HasWarnings)
{
    // Decide whether missing destination columns or missing order columns are acceptable.
}

var result = await new DbaTableCopyEngine().CopyAsync(
    sourceAdapter,
    destinationAdapter,
    plan.Definitions,
    new DbaTableCopyOptions
    {
        PageSize = 10000,
        BatchSize = 5000,
        ClearDestination = true
    },
    cancellationToken);
```

The planner is intentionally metadata-driven, not domain-driven. It can infer order columns from primary keys, omit generated/rowversion columns, omit destination identity columns when requested, match destination columns, and apply per-table mappings, exclusions, and conversions. Consumers such as TestimoX should still own their table order and domain schema rules.

For provider-backed .NET copies, reference `DBAClientX.DataMovement` and use `DbaProviderTableCopyAdapter` instead of implementing `IDbaTableCopySource` and `IDbaTableCopyDestination` yourself:

```csharp
var sourceAdapter = new DbaProviderTableCopyAdapter(
    DbaTableCopyProvider.SQLite,
    "Data Source=C:\\Data\\monitoring.sqlite",
    treatMissingTablesAsEmpty: true);

var destinationAdapter = new DbaProviderTableCopyAdapter(
    DbaTableCopyProvider.SqlServer,
    "Server=sql01;Database=monitoring;Encrypt=True;TrustServerCertificate=True;Integrated Security=True",
    sqlServerOptions: new SqlServerBulkInsertOptions
    {
        BulkCopyOptions = Microsoft.Data.SqlClient.SqlBulkCopyOptions.TableLock
    },
    treatMissingTablesAsEmpty: true);

var result = await new DbaTableCopyEngine().CopyAsync(
    sourceAdapter,
    destinationAdapter,
    plan.Definitions,
    new DbaTableCopyOptions
    {
        PageSize = 10000,
        BatchSize = 5000
    },
    cancellationToken);
```

The provider adapter supports SQL Server, PostgreSQL, MySQL, Oracle, and SQLite as either source or destination. PowerShell's `Copy-DbaXTableData` uses the same adapter internally and only maps user-friendly parameters into the reusable .NET API.

PowerShell can also run prepared definitions directly:

```powershell
$columnMap = [System.Collections.Generic.Dictionary[string,string]]::new()
$columnMap['DisplayName'] = 'ProbeDisplayName'

$typeConversions = [System.Collections.Generic.Dictionary[string,DBAClientX.DataMovement.DbaTableCopyColumnType]]::new()
$typeConversions['IsMaintenance'] = [DBAClientX.DataMovement.DbaTableCopyColumnType]::Boolean

$probeIndexSourceOptions = [DBAClientX.DataMovement.DbaTableCopySourceOptions]::new(
    [string[]] @('ProbeName'),
    [string[]] @('LastCompletedUtcMs'),
    $true)

$definitions = @(
    [DBAClientX.DataMovement.DbaTableCopyDefinition]::new(
        'main.ProbeResults',
        'dbo.ProbeResults',
        [string[]] @('ResultId'),
        'ProbeResults',
        $columnMap,
        [string[]] @('__MigrationRowId', '__MigrationRank'),
        $typeConversions)
    [DBAClientX.DataMovement.DbaTableCopyDefinition]::new(
        'main.ProbeIndex',
        'dbo.ProbeIndex',
        [string[]] @('ProbeName'),
        'ProbeIndex',
        $null,
        $null,
        $null,
        $probeIndexSourceOptions)
)

Copy-DbaXTableData `
    -SourceProvider SQLite `
    -SourceConnectionString 'Data Source=C:\Data\monitoring.sqlite' `
    -DestinationProvider SqlServer `
    -DestinationConnectionString 'Server=sql01;Database=monitoring;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -Definition $definitions `
    -BatchSize 5000 `
    -TableLock `
    -PassThru
```

## Benchmarking

Use the benchmark example to compare DbaClientX against locally installed PowerShell competitors without adding dependencies to the repo:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Server localhost -RowCount 5000 -Iterations 3
```

The script always benchmarks `Write-DbaXTableData`. If `Write-DbaDbTableData` from `dbatools` or `Write-SqlTableData` from the Microsoft `SqlServer` module is already installed, it adds those runs. Otherwise it reports only DbaClientX results.

Interpret the numbers as local evidence, not a universal ranking. SQL Server version, disk, network, TLS, table indexes, triggers, recovery model, batch size, and client runtime can dominate the result.

See [`sqlserver-benchmark-notes.md`](sqlserver-benchmark-notes.md) for local comparison evidence and dbatools behavior worth considering in future DbaClientX provider work.
