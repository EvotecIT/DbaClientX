# DbaClientX benchmarks

The suite uses BenchmarkDotNet with `MemoryDiagnoser`. Every benchmark returns or validates a result so an unexpectedly fast lane cannot silently skip the requested work.

- `QueryCompilerCacheBenchmarks` compares a fresh SQL shape with a cached shape while checking that each cache hit returns the current parameter values.
- `TableCopyPagingBenchmarks` measures in-memory orchestration and allocation only. It does not represent network or database-provider throughput.
- `RecordMappingBenchmarks` compares automatic, result-bound and hand-written typed mapping over 25,000 rows with 8 or 40 columns. Input creation, reader setup and schema binding occur outside timing; each iteration verifies the row count and a checksum covering mapped values. The workload includes numeric, enum, GUID, timestamp and nullable conversions. It measures mapping CPU/allocation, without database I/O.

```powershell
dotnet run --project DbaClientX.Benchmarks -c Release -- --filter '*QueryCompilerCacheBenchmarks*' --job Short --noOverwrite
```

Use `--job Dry` first after changing the benchmark or copy contracts. Treat ratios as same-run regression signals; absolute timings vary by machine. Do not publish machine-to-machine comparisons without recording CPU topology, operating system, power plan, runtime, affinity, and the exact matrix.
