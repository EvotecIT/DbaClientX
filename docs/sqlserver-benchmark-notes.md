# SQL Server benchmark notes

The SQL Server data-movement benchmark is a PSPublishModule/PowerForge benchmark suite, not a hand-rolled timing loop. `Module/Examples/Benchmark.SqlServerDataMovement.ps1` declares the SQL Server scenarios and provider operations, and the shared runner handles warmup iterations, measured iterations, rotated ordering, normalized artifacts, comparison output, and README block updates.

Run the benchmark:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000, 5000, 20000, 100000 `
    -BatchSize 5000 `
    -InputKind DataTable, DataReader, PSCustomObject, Class `
    -WarmupCount 5 `
    -Iterations 20
```

Run only one side of the suite when you want a smaller pass:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Operation Write -RowCount 100000
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Operation Read -RowCount 100000
```

Use `-Plan` to inspect the matrix without touching SQL Server:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Plan
```

The write suite benchmarks DbaClientX `Write-DbaXTableData` across `DataTable`, `DataReader`, `PSCustomObject`, and typed class input shapes. It compares DbaClientX with dbatools `Write-DbaDbTableData` and SqlServer `Write-SqlTableData` on their supported client-side input shapes. The `DataReader` lane is DbaClientX-only because it measures the public streaming path into SQL Server bulk copy. The dbatools `DataTable` lane passes a direct value to `-InputObject`, matching dbatools' documented SqlBulkCopy fast path and avoiding the slower piped `DataRow` path. `Copy-DbaDbTableData` is intentionally not part of this matrix because it measures SQL table-to-table streaming rather than client-side object/DataTable import.

The read suite seeds an isolated SQL Server table outside the measured operation, then compares DbaClientX `Invoke-DbaXQuery` with dbatools `Invoke-DbaQuery`. By default it reads every row as full-result `DataTable` and PowerShell-object output; pass `-ReadShape DataSetAll` to include a `DataSet` materialization lane for local diagnosis. Successful lanes verify row count plus simple data integrity (`Id` min/max/sum and `Score` sum) and then drop their isolated table. Failed lanes keep their table so the failing state can be inspected. Full `DataTable` materialization is allocation-heavy and can form GC timing clusters, so the default five warmups and twenty measurements are intentional; do not reduce them for a release comparison.

## CSV export and office round trips

`Benchmark.SqlServerCsvExport.ps1` measures export-only throughput from SQL
Server to CSV. The DbaClientX reader lane uses the public
`Invoke-DbaXQuery -AsDataReader` API and passes its owned SQL Server reader
directly to PSWriteOffice `Export-OfficeCsv`; the buffered lane uses a
`DataTable`; the stream lane uses the public `Invoke-DbaXQuery -Stream` shape;
and the partitioned lane opens one reader per partition and writes split CSV
files. Comparison lanes cover dbatools `Export-DbaCsv`, native `bcp queryout`,
and FastBCP.

`Benchmark.OfficeFileRoundTrip.ps1` measures the combined database/file
workflow: read source rows with DbaClientX, write CSV, compressed CSV, or Excel
with PSWriteOffice, import the file back as a tabular reader, then bulk-write to
SQL Server through `Write-DbaXTableData`. Equivalent comparison lanes cover
dbatools and native PowerShell for plain CSV, and ImportExcel for XLSX.
Comparison lanes use the same source rows, destination contract, batch size,
table-lock setting, and integrity checks. A shape remains a DbaClientX-only lane
when another engine cannot perform the same contract without an artificial
adapter. In typed CSV comparisons,
PSWriteOffice receives the declared column types while dbatools uses its public
`DetectColumnTypes` workflow; the runner records that API difference and still
requires the same typed destination values.

Run the export comparison:

```powershell
.\Module\Examples\Benchmark.SqlServerCsvExport.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 100000 `
    -Engine DbaClientXReader,dbatools,bcp,FastBCP `
    -Iterations 10 `
    -UpdateReadme
```

Run the CSV round-trip comparison:

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 100000 `
    -FileKind Csv,CsvGZip,CsvTyped,CsvGZipTyped `
    -ColumnShape Default,Mapped `
    -Engine DbaClientX,dbatools `
    -WarmupCount 3 `
    -Iterations 15 `
    -UpdateReadme
```

Add bounded ordered projection to both typed CSV import lanes with the same
worker and batch limits:

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 100000 `
    -FileKind CsvTyped `
    -Engine DbaClientX,dbatools `
    -ParallelCsvRead `
    -CsvReaderMaxDegreeOfParallelism 4 `
    -CsvReaderParallelBatchSize 4096 `
    -WarmupCount 3 `
    -Iterations 15
```

The parallel controls are recorded in benchmark metadata. They affect only the
typed CSV and typed compressed-CSV cases; other file kinds retain their normal
reader path.

Run the Excel round-trip lanes:

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000,5000,25000 `
    -FileKind ExcelReader `
    -Engine DbaClientX,ImportExcel `
    -Iterations 5
```

The runner can prepare ImportExcel in its benchmark-only module cache. Use
`-SkipImportExcelInstall` when benchmark execution must not download optional
comparison dependencies, or pass `-ImportExcelModulePath` to select an existing
module explicitly. Add `-ImportExcelVersion` to require a specific version;
the run fails or skips that lane instead of silently substituting another
version.

### Dated streaming XLSX round-trip snapshot (2026-08-10)

This source-linked run measured the complete 25,000-row SQL Server to XLSX to
SQL Server job. The DbaClientX lane streamed an owned database reader into
PSWriteOffice, imported the workbook as an OfficeIMO reader, and streamed that
reader into SQL Server bulk copy. The comparison lane used ImportExcel's public
object pipeline for the same source query, XLSX table, destination table, and
bulk-copy settings. These are equivalent user jobs, but not identical internal
input shapes.

The runner used PowerShell 7.6.4, SQL Server 17.0.1125.2 Standard Edition,
`High` process priority, five warmups, fifteen rotated measured iterations, no
removed outliers, and one 16-logical-processor L3 domain at a time. Every one
of the 60 measured round trips passed the 25,000-row count, min/max, integer
sum, decimal sum, destination schema, and strict full-join value comparison.
The strict comparison rejects fractional IDs and sub-cent score changes and
uses binary, length-aware text equality.

| Engine | L3 domain 0 median | L3 domain 1 median | Rows/s at median, domain 0 | Rows/s at median, domain 1 |
| --- | ---: | ---: | ---: | ---: |
| DbaClientX + PSWriteOffice + OfficeIMO | 218.62 ms | 174.12 ms | 114,355 | 143,580 |
| ImportExcel 7.8.10 | 3,519.73 ms | 2,926.71 ms | 7,103 | 8,542 |
| ImportExcel / DbaClientX duration | 16.10x | 16.81x | n/a | n/a |

ImportExcel 7.8.10 bundled EPPlus 4.5.3.2 in this run. The DbaClientX path
therefore used 5.9-6.2% of that public PowerShell workflow's median duration on
the two measured domains. This is not a comparison with current commercial
EPPlus, whose direct .NET API remains covered by the OfficeIMO.Excel library
benchmarks.

The product source candidates were DbaClientX `8241ccaa`, PSWriteOffice
`36b00b6e`, and OfficeIMO `21ac64cb`. The benchmark metadata records the
resolved module paths and versions, SQL Server identity, benchmark-script hash,
benchmark-support hash, and SHA-256 identity of the DbaClientX, PSWriteOffice,
OfficeIMO, and EPPlus assemblies. To reproduce the source-linked run, use
separate worktrees at those commits and the current benchmark harness:

```powershell
$harnessRoot = (Resolve-Path '.').Path
$dbaClientXRoot = '<path-to-DbaClientX-8241ccaa-worktree>'
$psWriteOfficeRoot = '<path-to-PSWriteOffice-36b00b6e-worktree>'
$officeIMORoot = '<path-to-OfficeIMO-21ac64cb-worktree>'
$importExcelCache = Join-Path $harnessRoot 'Ignore\Benchmarks\Modules'

dotnet build (Join-Path $dbaClientXRoot 'DbaClientX.PowerShell\DbaClientX.PowerShell.csproj') -c Release -f net8.0
dotnet build (Join-Path $psWriteOfficeRoot 'Sources\PSWriteOffice\PSWriteOffice.csproj') `
    -c Release -f net8.0 `
    -p:UseOfficeIMOProjectReferences=true `
    -p:OfficeIMORoot=$officeIMORoot
Save-Module ImportExcel -RequiredVersion 7.8.10 -Path $importExcelCache -Force

$env:DBACLIENTX_USE_DEVELOPMENT_BINARIES = 'true'
$env:DBACLIENTX_DEVELOPMENT_CONFIGURATION = 'Release'
$env:PSWRITEOFFICE_USE_DEVELOPMENT_BINARIES = 'true'
$env:PSWRITEOFFICE_DEVELOPMENT_CONFIGURATION = 'Release'

& (Join-Path $harnessRoot 'Module\Examples\Benchmark.OfficeFileRoundTrip.ps1') `
    -Server localhost `
    -Database tempdb `
    -RowCount 25000 `
    -FileKind ExcelReader `
    -ColumnShape Default `
    -Engine DbaClientX,ImportExcel `
    -WarmupCount 5 `
    -Iterations 15 `
    -ModulePath (Join-Path $dbaClientXRoot 'Module\DbaClientX.psd1') `
    -PSWriteOfficeModulePath (Join-Path $psWriteOfficeRoot 'PSWriteOffice.psd1') `
    -ImportExcelModulePath (Join-Path $importExcelCache 'ImportExcel\7.8.10\ImportExcel.psd1') `
    -ImportExcelVersion 7.8.10 `
    -SkipImportExcelInstall `
    -ProcessorAffinity 0xFFFF `
    -ProcessPriority High

# Repeat with -ProcessorAffinity 0xFFFF0000 for the second measured domain.
```

On machines with heterogeneous CPU domains, pin comparable runs to the same
native-width processor mask and record the applied process settings in the
benchmark metadata:

```powershell
.\Module\Examples\Benchmark.OfficeFileRoundTrip.ps1 `
    -RowCount 100000 `
    -FileKind Csv,ExcelReader `
    -Engine DbaClientX,NativePowerShell,ImportExcel `
    -ProcessorAffinity 0x000000000000FFFF `
    -ProcessPriority AboveNormal
```

`Benchmark.SqlServerDataMovement.ps1` and
`Benchmark.SqlServerCsvExport.ps1` accept the same process controls. A
partitioned CSV export is reported as its own engine; do not assume it is faster
than one streaming reader unless the measured row count and SQL partitioning
strategy prove it.

For unreleased work, build PSWriteOffice with `OfficeIMORoot` pointing at the
current OfficeIMO checkout and pass `-ModulePath` / `-PSWriteOfficeModulePath`
to these scripts. That keeps DbaClientX, PSWriteOffice, and OfficeIMO on the
exact source under test. Published-package measurements belong to a later
release audit, after matching packages exist.

Artifacts are written under `Ignore\Benchmarks\SqlServerDataMovement\Write` and `Ignore\Benchmarks\SqlServerDataMovement\Read`:

- `samples.json` / `samples.csv`
- `summary.json` / `summary.csv`
- `comparison.json` / `comparison.md`
- `metadata.json`
- `run-report.json`

The timing artifacts include the machine, host runtime, and measured matrix so
results can be compared with later runs on the same environment.

## SQL Server import controls

Keep the default import path small. Add SQL Server controls only when the destination table requires them:

| When you need to... | Use |
| --- | --- |
| Create the missing destination schema/table from the incoming `DataTable` shape | `Write-DbaXTableData -AutoCreateTable` |
| Rename source columns for the destination table | `Write-DbaXTableData -ColumnMap` or `SqlServerBulkInsertOptions.ColumnMappings` |
| Ask SQL Server for a table lock during the load | `-TableLock` |
| Enforce constraints or fire triggers during the load | `-CheckConstraints` / `-FireTriggers` |
| Preserve incoming identity values or nulls | `-KeepIdentity` / `-KeepNulls` |
| Show progress for a longer import | `-NotifyAfter` |

PowerShell input conversion is deliberately small: `TimeSpan` values stay scalar, scalar input becomes a `Value` column, and a single enumerable input expands into rows.

File-format-specific conversion should stay in the owning file-format library. DbaClientX should receive a shaped `DataTable`, `IDataReader`, or object stream and own the provider write.
