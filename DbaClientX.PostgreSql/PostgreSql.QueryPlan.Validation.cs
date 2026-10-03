using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;
using NpgsqlTypes;

namespace DBAClientX;

public partial class PostgreSql
{
    private static (string Statement, Dictionary<string, object?>? Values, Dictionary<string, NpgsqlDbType>? Types)
        ValidateQueryPlanInput(string query, IDictionary<string, object?>? parameters, IDictionary<string, NpgsqlDbType>? parameterTypes)
    {
        if (string.IsNullOrWhiteSpace(query) || query.Length > 1024 * 1024)
            throw new ArgumentException("A statement of at most 1,048,576 characters is required.", nameof(query));
        var statements = SqlStatementText.Split(query, SqlDialect.PostgreSql);
        if (statements.Count != 1) throw new ArgumentException("Estimated capture requires one DML statement.", nameof(query));
        var tokens = SqlTokenizer.Tokenize(statements[0], dollarQuotes: true, nestedBlockComments: true, bracketIdentifiers: false);
        if (tokens.Count == 0 || tokens[0].Kind != SqlTokenKind.Word || tokens[0].Text.ToUpperInvariant() is not
            ("SELECT" or "WITH" or "INSERT" or "UPDATE" or "DELETE" or "MERGE"))
            throw new ArgumentException("Only SELECT, WITH, INSERT, UPDATE, DELETE and MERGE statements can be explained.", nameof(query));
        var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var token in tokens.Where(token => token.Kind == SqlTokenKind.Parameter))
        {
            if (token.Text[0] is '@' or ':') required.Add(token.Text.Substring(1));
            else if (token.Text[0] == '$') throw new ArgumentException("Use named @parameters or :parameters for bound estimated plans.", nameof(query));
        }
        if (parameters?.Count > 65535) throw new ArgumentException("The native protocol supports at most 65535 parameters.", nameof(parameters));
        var values = parameters == null ? null : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (parameters != null)
            foreach (var pair in parameters)
            {
                var name = pair.Key.TrimStart('@', ':');
                if (!required.Contains(name) || values!.ContainsKey(name))
                    throw new ArgumentException("Every value must name one used, unambiguous parameter.", nameof(parameters));
                values.Add(name, pair.Value);
            }
        if (required.Any(name => values == null || !values.ContainsKey(name)))
            throw new ArgumentException("Every named placeholder needs a bound value.", nameof(parameters));
        var types = parameterTypes == null ? null : new Dictionary<string, NpgsqlDbType>(StringComparer.OrdinalIgnoreCase);
        if (parameterTypes != null)
            foreach (var pair in parameterTypes)
            {
                var name = pair.Key.TrimStart('@', ':');
                if (values == null || !values.ContainsKey(name) || types!.ContainsKey(name))
                    throw new ArgumentException("Every type must name one bound parameter.", nameof(parameterTypes));
                types.Add(name, pair.Value);
            }
        return (statements[0], values, types);
    }
}
