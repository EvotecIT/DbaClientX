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
    -Iterations 3
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

The write suite benchmarks DbaClientX `Write-DbaXTableData` across `DataTable`, `DataReader`, `PSCustomObject`, and typed class input shapes. It adds dbatools `Write-DbaDbTableData` and SqlServer `Write-SqlTableData` only when those commands are available. The `DataReader` lane is DbaClientX-only because it measures the public streaming path into SQL Server bulk copy; dbatools and the SqlServer module stay on their documented client-side input shapes. The dbatools `DataTable` lane passes a direct value to `-InputObject`, matching dbatools' documented SqlBulkCopy fast path and avoiding the slower piped `DataRow` path. `Copy-DbaDbTableData` is intentionally not part of this matrix because it measures SQL table-to-table streaming rather than client-side object/DataTable import.

The read suite seeds an isolated SQL Server table outside the measured operation, then compares DbaClientX `Invoke-DbaXQuery` with dbatools `Invoke-DbaQuery` when dbatools is available. By default it reads every row as full-result `DataTable` and PowerShell-object output; pass `-ReadShape DataSetAll` to include a `DataSet` materialization lane for local diagnosis. Successful lanes verify row count plus simple data integrity (`Id` min/max/sum and `Score` sum) and then drop their isolated table. Failed lanes keep their table so the failing state can be inspected.

## CSV export and office round trips

`Benchmark.SqlServerCsvExport.ps1` measures export-only throughput from SQL
Server to CSV. The DbaClientX reader lane opens a SQL Server `IDataReader` and
passes it directly to PSWriteOffice `Export-OfficeCsv`; the buffered lane uses a
`DataTable`; the stream lane uses the public `Invoke-DbaXQuery -Stream` shape;
and the partitioned lane opens one reader per partition and writes split CSV
files. Comparison lanes cover dbatools `Export-DbaCsv`, native `bcp queryout`,
and FastBCP when those tools are installed.

`Benchmark.OfficeFileRoundTrip.ps1` measures the combined database/file
workflow: read source rows with DbaClientX, write CSV, compressed CSV, or Excel
with PSWriteOffice, import the file back as a tabular reader, then bulk-write to
SQL Server through `Write-DbaXTableData`.
The dbatools comparison is CSV-only; Excel remains a DbaClientX + PSWriteOffice
lane.

The benchmark commands expect matching DbaClientX, PSWriteOffice, and OfficeIMO
packages. Use module-path parameters only when validating a local source build.

Artifacts are written under `Ignore\Benchmarks\SqlServerDataMovement\Write` and `Ignore\Benchmarks\SqlServerDataMovement\Read`:

- `samples.json` / `samples.csv`
- `summary.json` / `summary.csv`
- `comparison.json` / `comparison.md`
- `metadata.json`
- `run-report.json`

Treat all numbers as workstation-local evidence, not universal rankings. SQL Server version, disk, network, TLS, table indexes, triggers, recovery model, batch size, and client runtime can dominate the result.

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
