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
| 5,000 | 3 | DbaClientX `Write-DbaXTableData` | 26.94 ms to 121.67 ms |
| 5,000 | 3 | dbatools `Write-DbaDbTableData` | 19.12 s to 48.62 s |
| 20,000 | 1 | DbaClientX `Write-DbaXTableData` | 173.92 ms |
| 20,000 | 1 | dbatools `Write-DbaDbTableData` | 55.35 s |

These are workstation-local measurements, not universal rankings. They include each public cmdlet surface as called by the benchmark. The dbatools run used `Connect-DbaInstance -TrustServerCertificate` and a `System.Data.DataTable` input, so it was not failing certificate validation and was not intentionally forced through object-pipeline conversion.

## dbatools behaviors worth learning from

The installed dbatools 2.8.2 implementation shows several useful scenarios DbaClientX can consider over time:

- `ColumnMap` lets callers map source column names to different destination column names.
- `AutoCreateTable` can create missing schemas and destination tables.
- `TableLock` is enabled by default unless `-NoTableLock` is used.
- Bulk-copy options are exposed for `CheckConstraints`, `FireTriggers`, `KeepIdentity`, and `KeepNulls`.
- `NotifyAfter` and progress reporting are available for long-running imports.
- `ConvertTo-DbaDataTable` has configurable conversion for `TimeSpan`, dbatools size types, arrays, and raw string fallback.

DbaClientX should keep the common fast path small, but these are good candidates for provider-owned features instead of consumer-side workarounds when real callers need them.
