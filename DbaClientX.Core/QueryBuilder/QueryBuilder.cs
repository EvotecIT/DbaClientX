using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Provides helper methods to construct and compile SQL queries.
/// </summary>
public static class QueryBuilder
{
    /// <summary>
    /// Creates a new, empty <see cref="Query"/> instance.
    /// </summary>
    /// <returns>A new <see cref="Query"/>.</returns>
    public static Query Query() => new Query();

    /// <summary>
    /// Compiles the specified query to a SQL string.
    /// </summary>
    /// <param name="query">The query to compile.</param>
    /// <returns>The compiled SQL statement.</returns>
    public static string Compile(Query query)
    {
        var compiler = new QueryCompiler();
        return compiler.Compile(query);
    }

    /// <summary>
    /// Compiles the specified query to a SQL string and returns parameters separately.
    /// </summary>
    /// <param name="query">The query to compile.</param>
    /// <returns>A tuple containing the SQL statement and a list of parameters.</returns>
    public static (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters(Query query)
    {
        var compiler = new QueryCompiler();
        return compiler.CompileWithParameters(query);
    }
}

