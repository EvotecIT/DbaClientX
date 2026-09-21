# DbaClientX benchmarks

The suite uses BenchmarkDotNet with `MemoryDiagnoser`. Every benchmark returns or validates a result so an unexpectedly fast lane cannot silently skip the requested work.

- `QueryCompilerCacheBenchmarks` compares a fresh SQL shape with a cached shape while checking that each cache hit returns the current parameter values.
- `TableCopyPagingBenchmarks` measures in-memory orchestration and allocation only. It does not represent network or database-provider throughput.

```powershell
dotnet run --project DbaClientX.Benchmarks -c Release -- --filter '*QueryCompilerCacheBenchmarks*' --job Short --noOverwrite
```

Use `--job Dry` first after changing the benchmark or copy contracts. Treat ratios as same-run regression signals; absolute timings vary by machine. Do not publish machine-to-machine comparisons without recording CPU topology, operating system, power plan, runtime, affinity, and the exact matrix.
