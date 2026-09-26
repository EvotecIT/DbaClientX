using System;
using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

/// <summary>
/// Converts positional values from <see cref="Query.CompileWithParameters(SqlDialect)"/> into the named parameter
/// dictionaries the provider query and streaming APIs accept.
/// </summary>
public static class QueryParameters
{
    /// <summary>
    /// Creates a dictionary keyed by the placeholders the compiler emits: <c>@p0</c>, <c>@p1</c>, … for SQL Server,
    /// PostgreSQL, MySQL and SQLite, and <c>:p0</c>, <c>:p1</c>, … for Oracle.
    /// </summary>
    /// <param name="values">Positional values in placeholder order.</param>
    /// <param name="dialect">The dialect the SQL was compiled for.</param>
    /// <returns>A new dictionary that can be passed as <c>parameters</c> to <c>Query*</c> and <c>QueryStream*</c> methods.</returns>
    public static Dictionary<string, object?> ToDictionary(IReadOnlyList<object> values, SqlDialect dialect)
    {
        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        var parameters = new Dictionary<string, object?>(values.Count, StringComparer.OrdinalIgnoreCase);
        for (var index = 0; index < values.Count; index++)
        {
            parameters.Add(QueryCompiler.GetParameterName(dialect, index), values[index]);
        }

        return parameters;
    }
}
