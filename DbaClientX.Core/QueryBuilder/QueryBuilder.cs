using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public static class QueryBuilder
{
    public static Query Query() => new Query();

    public static string Compile(Query query)
    {
        var compiler = new QueryCompiler();
        return compiler.Compile(query);
    }

    public static (string Sql, List<object> Parameters) CompileWithParameters(Query query)
    {
        var compiler = new QueryCompiler();
        return compiler.CompileWithParameters(query);
    }
}

