using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Provides factory helpers for building and compiling SQL queries.
/// </summary>
public static class QueryBuilder
{
    /// <summary>
    /// Creates a new query instance.
    /// </summary>
    /// <returns>A new <see cref="Query"/>.</returns>
    public static Query Query() => new Query();

    /// <summary>
    /// Compiles a query into SQL text using the specified dialect.
    /// </summary>
    /// <param name="query">The query to compile.</param>
    /// <param name="dialect">The target SQL dialect.</param>
    /// <returns>The SQL text representation.</returns>
    public static string Compile(Query query, SqlDialect dialect = SqlDialect.SqlServer)
    {
        var compiler = new QueryCompiler(dialect);
        return compiler.Compile(query);
    }

    /// <summary>
    /// Compiles a query into SQL text and captures the associated parameter values.
    /// </summary>
    /// <param name="query">The query to compile.</param>
    /// <param name="dialect">The target SQL dialect.</param>
    /// <returns>A tuple containing the SQL text and ordered parameter values.</returns>
    public static (string Sql, IReadOnlyList<object> Parameters) CompileWithParameters(Query query, SqlDialect dialect = SqlDialect.SqlServer)
    {
        var compiler = new QueryCompiler(dialect);
        return compiler.CompileWithParameters(query);
    }
}

