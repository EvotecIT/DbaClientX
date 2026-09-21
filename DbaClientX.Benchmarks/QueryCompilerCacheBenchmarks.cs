using BenchmarkDotNet.Attributes;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Benchmarks;

/// <summary>Measures parameterized compilation with and without a reusable SQL-shape cache entry.</summary>
[MemoryDiagnoser]
public class QueryCompilerCacheBenchmarks
{
    private readonly QueryCompiler _compiler = new(SqlDialect.PostgreSql);
    private int _value;
    private int _shape;

    [GlobalSetup]
    public void Setup()
    {
        QueryCompiler.ClearCache();
        _compiler.CompileWithParameters(CreateQuery("users", 0));
    }

    [Benchmark(Baseline = true)]
    public int CompileNewShape()
    {
        var shape = Interlocked.Increment(ref _shape);
        var compiled = _compiler.CompileWithParameters(CreateQuery("users_" + shape, shape));
        return Validate(compiled, shape);
    }

    [Benchmark]
    public int CompileCachedShape()
    {
        var value = Interlocked.Increment(ref _value);
        var compiled = _compiler.CompileWithParameters(CreateQuery("users", value));
        return Validate(compiled, value);
    }

    private static Query CreateQuery(string table, int tenant)
        => new Query()
            .Select("id", "display_name")
            .From(table)
            .Where("tenant_id", tenant)
            .Where("enabled", true)
            .OrderBy("id")
            .Limit(100);

    private static int Validate((string Sql, IReadOnlyList<object> Parameters) compiled, int expectedTenant)
    {
        if (compiled.Parameters.Count != 2 ||
            Convert.ToInt32(compiled.Parameters[0]) != expectedTenant ||
            !Equals(compiled.Parameters[1], true))
        {
            throw new InvalidOperationException("The compiled query did not preserve the current parameter values.");
        }

        return compiled.Sql.Length + compiled.Parameters.Count;
    }
}
