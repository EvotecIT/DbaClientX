# SQL Server benchmark notes

Use `Module/Examples/Benchmark.SqlServerDataMovement.ps1` for local evidence. The script creates isolated tables in the target database, runs each tool for the requested number of iterations, verifies row counts after each write, and drops the tables unless `-KeepTables` is used.

Example:

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 20000 `
    -Iterations 1 `
    -BatchSize 5000
```

## Local evidence

On the local `localhost` SQL Server used while preparing this branch:

| Rows | Iterations | Tool | Elapsed |
| ---: | ---: | --- | ---: |
| 1,000 | 1 | DbaClientX `Write-DbaXTableData` | 87.56 ms |
| 1,000 | 1 | dbatools `Write-DbaDbTableData` | 2.20 s |
| 5,000 | 1 | DbaClientX `Write-DbaXTableData` | 103.64 ms |
| 5,000 | 1 | dbatools `Write-DbaDbTableData` | 10.05 s |
| 5,000 | 3 | DbaClientX `Write-DbaXTableData` | 26.94 ms to 121.67 ms |
| 5,000 | 3 | dbatools `Write-DbaDbTableData` | 19.12 s to 48.62 s |
| 20,000 | 1 | DbaClientX `Write-DbaXTableData` | 173.92 ms |
| 20,000 | 1 | dbatools `Write-DbaDbTableData` | 55.35 s |

These are workstation-local measurements, not universal rankings. They include each public cmdlet surface as called by the benchmark. The comparison run used `Connect-DbaInstance -TrustServerCertificate` and a `System.Data.DataTable` input, so it was not failing certificate validation and was not intentionally forced through object-pipeline conversion. The 1,000-row and 5,000-row single-iteration rows above use explicit `-InputObject` for both cmdlets.

For deeper performance work, run each command in separate PowerShell processes and include a raw `SqlBulkCopy` baseline.

## SQL Server import controls

DbaClientX exposes the SQL Server import controls that are useful for reusable staging and migration work while keeping the default path small:

- `AutoCreateTable` creates missing schemas and destination tables from the incoming `DataTable` shape.
- `ColumnMap` maps source column names to different destination column names through `Write-DbaXTableData -ColumnMap` and provider-level `SqlServerBulkInsertOptions.ColumnMappings`.
- SQL Server bulk-copy options are exposed for `TableLock`, `CheckConstraints`, `FireTriggers`, `KeepIdentity`, and `KeepNulls`.
- `NotifyAfter` and progress reporting are available for long-running SQL Server imports.
- PowerShell input conversion preserves `TimeSpan` scalar values, treats scalar input as a `Value` column, and expands a single enumerable input into rows.

File-format-specific conversion should stay in the owning file-format library. DbaClientX should receive a shaped `DataTable`, `IDataReader`, or object stream and own the provider write.
