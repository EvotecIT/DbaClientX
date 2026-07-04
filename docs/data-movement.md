# DbaClientX data movement

Use this guide when a script or service needs to move rows into or between databases. Keep higher-level tools focused on their own domain work, then hand tabular data to DbaClientX for provider-specific writes.

Pick the flow that matches the job:

- SQL Server staging tables for reports, inventories, or synchronization jobs.
- PSWriteOffice import/export jobs that read Excel into `DataTable` objects and bulk-write them to a database.
- Cross-provider scripts where the same PowerShell shape writes to SQL Server, PostgreSQL, MySQL, SQLite, or Oracle.
- Table-to-table migrations between provider connections, such as SQLite history stores moving into SQL Server.
- .NET services that should reference provider packages through DbaClientX instead of repeating provider-specific connection and bulk-copy code.

## Prove SQL Server Writes On A Workstation

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

## Move Rows From A File Into A Database

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

Run this when you want to prove the full SQL Server -> Excel -> SQL Server path:

```powershell
.\Module\Examples\Example.ExcelRoundTrip.ps1 -Server localhost -Database tempdb -RowCount 100 -KeepArtifacts
```

That example exports SQL Server rows to an `.xlsx` workbook with PSWriteOffice, imports the workbook back as a `DataTable`, bulk-writes the data through DbaClientX, and fails if the exported, imported, written, source, and destination row counts do not match.

## Load A SQL Server Staging Table

Start with the default `Write-DbaXTableData` path, then add SQL Server options only when the destination table needs them:

```powershell
Write-DbaXTableData `
    -Provider SqlServer `
    -ConnectionString 'Server=sql01;Database=warehouse;Encrypt=True;TrustServerCertificate=True;Integrated Security=True' `
    -DestinationTable 'staging.Customers' `
    -InputObject $customerTable `
    -AutoCreateTable `
    -ColumnMap @{ CustomerName = 'DisplayName'; CustomerId = 'Id' } `
    -TableLock `
    -KeepIdentity `
    -KeepNulls `
    -NotifyAfter 5000 `
    -PassThru
```

Use these options for the matching SQL Server requirement:

| Requirement | Parameter |
| --- | --- |
| Create a missing schema/table from the incoming tabular shape | `-AutoCreateTable` |
| Rename source columns for the destination table | `-ColumnMap` |
| Take a bulk-update table lock during the load | `-TableLock` |
| Enforce constraints during the bulk copy | `-CheckConstraints` |
| Fire insert triggers during the bulk copy | `-FireTriggers` |
| Preserve incoming identity values | `-KeepIdentity` |
| Preserve incoming nulls instead of destination defaults | `-KeepNulls` |
| Report progress during longer imports | `-NotifyAfter` |

These switches are SQL Server-specific. Other providers reject them instead of silently ignoring them. `-AutoCreateTable` creates missing schemas and destination tables from the incoming `DataTable` shape; existing tables are left unchanged.

PowerShell input conversion is intentionally small. `TimeSpan` values are preserved as scalar values, scalar pipeline input becomes a `Value` column, and a single enumerable input expands into rows. When a source format needs richer coercion, keep that logic in the owning file-format library and pass DbaClientX a shaped `DataTable`, `IDataReader`, or object stream.

## Write To Another Provider

The cmdlet stays the same; change the provider and connection string.

```powershell
$rows | Write-DbaXTableData `
    -Provider PostgreSql `
    -ConnectionString 'Host=localhost;Database=app;Username=user;Password=secret;SslMode=Require' `
    -DestinationTable 'public.import_customers' `
    -BatchSize 5000

$rows | Write-DbaXTableData `
    -Provider MySql `
    -ConnectionString 'Server=localhost;Database=app;User ID=user;Password=secret;SslMode=Required;AllowLoadLocalInfile=true' `
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

## Copy A Table Between Providers

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

When providers differ in type affinity or destination schema, shape each page before the bulk write. Use this shape for SQLite to SQL Server history migrations where SQLite helper columns should be dropped, identity columns should be omitted, and numeric flags should become SQL Server `bit` values:

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

When `DbaTableCopyOptions.ClearDestination` is enabled for a multi-table plan, DbaClientX clears destination tables in reverse definition order before copying rows in the declared order. That lets domain tools list parent tables before child/detail tables for natural copy order while still deleting dependent destination rows first.

For provider-backed .NET copies, reference `DBAClientX.Core` plus only the provider packages you need. The runner and neutral contracts live in Core, while concrete adapters live in provider packages such as `DBAClientX.SQLite` and `DBAClientX.SqlServer`:

```csharp
var runner = new DbaProviderTableCopyRunner(
    source => source.Provider switch
    {
        DbaTableCopyProvider.SQLite => new SQLiteTableCopyAdapter(source),
        _ => throw new NotSupportedException($"Source provider '{source.Provider}' is not enabled.")
    },
    destination => destination.Provider switch
    {
        DbaTableCopyProvider.SqlServer => new SqlServerTableCopyAdapter(
            destination,
            new SqlServerBulkInsertOptions
            {
                BulkCopyOptions = Microsoft.Data.SqlClient.SqlBulkCopyOptions.TableLock
            }),
        _ => throw new NotSupportedException($"Destination provider '{destination.Provider}' is not enabled.")
    });

var result = await runner.CopyAsync(new DbaProviderTableCopyRequest
{
    Source = new DbaProviderTableCopyAdapterOptions
    {
        Provider = DbaTableCopyProvider.SQLite,
        ConnectionString = "Data Source=C:\\Data\\monitoring.sqlite",
        TreatMissingTablesAsEmpty = true
    },
    Destination = new DbaProviderTableCopyAdapterOptions
    {
        Provider = DbaTableCopyProvider.SqlServer,
        ConnectionString = "Server=sql01;Database=monitoring;Encrypt=True;TrustServerCertificate=True;Integrated Security=True",
        TreatMissingTablesAsEmpty = true
    },
    Definitions = plan.Definitions,
    Options = new DbaTableCopyOptions
    {
        PageSize = 10000,
        BatchSize = 5000
    }
}, cancellationToken);
```

The provider runner is intentionally factory-based: there is no required all-provider data-movement package. Applications reference only the provider adapters they compile in. PowerShell's `Copy-DbaXTableData` references all provider packages because the module intentionally offers all provider choices, but application services such as TestimoX can compile only SQLite and SQL Server.

By default, the provider runner refuses a same-provider copy when the normalized source and destination database identity and table name are the same. This prevents accidental self-copy loops or destructive clears during migration work. In rare advanced scenarios, set `DbaProviderTableCopyRequest.AllowSameProviderTableCopy = true`; in PowerShell, use `-AllowSameTableCopy`.

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

## Benchmark SQL Server Imports

Use the benchmark example for repeatable local SQL Server import evidence. The old example path is now a thin wrapper over the PSPublishModule benchmark suite that ships beside it in `Module/Examples`.

```powershell
Install-Module PSPublishModule -MinimumVersion 3.0.42 -Scope CurrentUser

.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000, 5000, 20000 `
    -BatchSize 5000 `
    -Iterations 3
```

The suite always benchmarks `Write-DbaXTableData`. If optional comparison commands are already installed, it adds dbatools `Write-DbaDbTableData` and SqlServer `Write-SqlTableData` lanes. Otherwise those lanes are skipped by the shared runner instead of being treated as failures.

Use `-Plan` to inspect the resolved benchmark matrix without creating SQL Server tables:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Plan
```

Interpret the numbers as local evidence, not a universal ranking. SQL Server version, disk, network, TLS, table indexes, triggers, recovery model, batch size, and client runtime can dominate the result.

The README includes a benchmark block that the suite can refresh with a normalized comparison table. JSON, CSV, and Markdown artifacts are written under `Ignore\Benchmarks\SqlServerDataMovement`.
