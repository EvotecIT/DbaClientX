using BenchmarkDotNet.Attributes;

namespace DbaClientX.Benchmarks;

[MemoryDiagnoser]
[InvocationCount(1)]
public class RecordMappingBenchmarks
{
    private RecordMappingWorkload _workload = null!;

    [Params(8, 40)]
    public int Columns { get; set; }

    [Params(25_000)]
    public int Rows { get; set; }

    [GlobalSetup]
    public void Setup() => _workload = new RecordMappingWorkload(Rows, Columns);

    [IterationSetup(Target = nameof(Automatic))]
    public void PrepareAutomatic() => _workload.Prepare(_workload.Automatic);

    [IterationSetup(Target = nameof(Bound))]
    public void PrepareBound() => _workload.Prepare(_workload.Bound);

    [IterationSetup(Target = nameof(Manual))]
    public void PrepareManual() => _workload.Prepare(RecordMappingWorkload.Manual);

    [Benchmark(Baseline = true)]
    public long Automatic() => _workload.MapAll(_workload.Automatic);

    [Benchmark]
    public long Bound() => _workload.MapAll(_workload.Bound);

    [Benchmark]
    public long Manual() => _workload.MapAll(RecordMappingWorkload.Manual);

    [IterationCleanup]
    public void Validate() => _workload.Validate();

    [GlobalCleanup]
    public void Cleanup() => _workload.Dispose();
}
