# SQL Server benchmark notes

The SQL Server data-movement benchmark is a PSPublishModule/PowerForge benchmark suite, not a hand-rolled timing loop. DbaClientX declares the SQL Server scenarios and provider operations in `Module/Examples/Benchmark.SqlServerDataMovement.ps1`; the shared runner owns warmup iterations, measured iterations, rotated ordering, normalized artifacts, comparison output, and README block updates.

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
