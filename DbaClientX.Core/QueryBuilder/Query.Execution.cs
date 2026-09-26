using System.Collections.Generic;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>
    /// Compiles the query into SQL text and a named parameter dictionary ready for the provider query and streaming APIs.
    /// </summary>
    /// <param name="dialect">The target SQL dialect.</param>
    /// <returns>The SQL text and parameters keyed by placeholder (see <see cref="QueryParameters.ToDictionary"/>).</returns>
    public (string Sql, Dictionary<string, object?> Parameters) CompileWithNamedParameters(SqlDialect dialect = SqlDialect.SqlServer)
    {
        var (sql, values) = CompileWithParameters(dialect);
        return (sql, QueryParameters.ToDictionary(values, dialect));
    }
}
