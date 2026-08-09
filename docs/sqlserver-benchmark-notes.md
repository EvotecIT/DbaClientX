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
table-lock setting, and integrity checks. Typed, compressed, mapped, and
streaming-only shapes remain DbaClientX lanes when another engine cannot perform
the same contract without an artificial adapter.

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
module explicitly.

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
