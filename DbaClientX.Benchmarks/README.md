# DbaClientX benchmarks

The table-copy benchmark compares the cursor-driven engine with a frozen offset-loop baseline under the same in-memory paging workload.

```powershell
dotnet run --project DbaClientX.Benchmarks -c Release -- --filter '*TableCopyPagingBenchmarks*' --job Short --noOverwrite
```

Use `--job Dry` first after changing the benchmark or copy contracts. Treat ratios as same-run regression signals; absolute timings vary by machine.
