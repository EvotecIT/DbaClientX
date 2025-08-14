using System;
using System.Diagnostics;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        var query = new Query().Select("*").From("users").Where("id", 1);
        var compiler = new QueryCompiler(SqlDialect.SqlServer);

        QueryCompiler.ClearCache();
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < 10000; i++)
        {
            QueryCompiler.ClearCache();
            compiler.Compile(query);
        }
        sw.Stop();
        Console.WriteLine($"Uncached: {sw.ElapsedMilliseconds} ms");

        QueryCompiler.ClearCache();
        compiler.Compile(query); // populate cache
        sw.Restart();
        for (int i = 0; i < 10000; i++)
        {
            compiler.Compile(query);
        }
        sw.Stop();
        Console.WriteLine($"Cached: {sw.ElapsedMilliseconds} ms");
    }
}
