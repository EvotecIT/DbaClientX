# DbaClientX benchmarks

Benchmarks in this folder are PSPublishModule/PowerForge benchmark suite specs. DbaClientX should declare scenarios, provider engines, validation, and document targets here; timing policy, rotated execution, normalized artifacts, comparison rendering, and README block updates belong to the shared benchmark runner.

## SQL Server data movement

```powershell
.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 -Plan

.\Module\Examples\Benchmark.SqlServerDataMovement.ps1 `
    -Server localhost `
    -Database tempdb `
    -RowCount 1000, 5000, 20000 `
    -BatchSize 5000 `
    -Iterations 3
```

Artifacts are written under `Ignore\Benchmarks`, which is ignored by Git.
