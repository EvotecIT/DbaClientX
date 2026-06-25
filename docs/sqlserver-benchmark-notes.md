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

These are workstation-local measurements, not universal rankings. They include each public cmdlet surface as called by the benchmark. The dbatools run used `Connect-DbaInstance -TrustServerCertificate` and a `System.Data.DataTable` input, so it was not failing certificate validation and was not intentionally forced through object-pipeline conversion. The 1,000-row and 5,000-row single-iteration rows above use explicit `-InputObject` for both cmdlets. A separate dbatools-only spot check still measured seconds for a 1,000-row `DataTable` load, which suggests the observed gap is mostly wrapper/connection/SMO/progress overhead in the dbatools cmdlet path rather than an in-process DbaClientX module side effect.

Treat the comparison as "thin DbaClientX bulk-copy surface versus dbatools administrative import surface", not as a claim that every DbaClientX scenario is universally faster than every dbatools scenario. For deeper performance work, run each tool in separate PowerShell processes and include a raw `SqlBulkCopy` baseline.

## dbatools behaviors reviewed

The installed dbatools 2.8.2 implementation shows several useful scenarios for operator-friendly SQL Server imports. This branch brings the highest-value bulk-copy controls into DbaClientX's SQL Server provider while keeping the default path small:

- `ColumnMap` lets callers map source column names to different destination column names. DbaClientX exposes this as `Write-DbaXTableData -ColumnMap` and provider-level `SqlServerBulkInsertOptions.ColumnMappings`.
- SQL Server bulk-copy options are exposed for `TableLock`, `CheckConstraints`, `FireTriggers`, `KeepIdentity`, and `KeepNulls`.
- `NotifyAfter` and progress reporting are available for long-running SQL Server imports.

These dbatools behaviors remain good future candidates:

- `AutoCreateTable` can create missing schemas and destination tables.
- `ConvertTo-DbaDataTable` has configurable conversion for `TimeSpan`, dbatools size types, arrays, and raw string fallback.

DbaClientX should keep the common fast path small, but reusable provider-owned features are preferable to consumer-side workarounds when real callers need them.
