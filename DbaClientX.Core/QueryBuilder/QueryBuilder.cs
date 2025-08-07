using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public static class QueryBuilder
{
    public static Query Query() => new Query();

    public static string Compile(Query query, SqlDialect dialect = SqlDialect.SqlServer)
    {
        var compiler = new QueryCompiler(dialect);
        return compiler.Compile(query);
    }

    public static (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters(Query query, SqlDialect dialect = SqlDialect.SqlServer)
    {
        var compiler = new QueryCompiler(dialect);
        return compiler.CompileWithParameters(query);
    }
}

