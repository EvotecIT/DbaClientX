using DBAClientX.QueryPlans;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    private static (string Statement, Dictionary<string, object?>? Values, Dictionary<string, MySqlDbType>? Types)
        ValidateQueryPlanInput(string query, IDictionary<string, object?>? parameters, IDictionary<string, MySqlDbType>? parameterTypes)
    {
        if (string.IsNullOrWhiteSpace(query) || query.Length > 1024 * 1024)
            throw new ArgumentException("A statement of at most 1,048,576 characters is required.", nameof(query));
        var tokens = SqlTokenizer.Tokenize(query, out var executableComments, backslashStrings: true, bracketIdentifiers: false);
        if (executableComments) throw new ArgumentException("Executable comments are unsupported in estimated capture.", nameof(query));
        var first = 0;
        while (first < tokens.Count && tokens[first].Kind == SqlTokenKind.Semicolon) first++;
        var last = tokens.Count - 1;
        while (last >= first && tokens[last].Kind == SqlTokenKind.Semicolon) last--;
        if (last < first || tokens.Skip(first).Take(last - first + 1).Any(token => token.Kind == SqlTokenKind.Semicolon))
            throw new ArgumentException("Estimated capture requires one DML statement.", nameof(query));
        if (tokens[first].Kind != SqlTokenKind.Word || tokens[first].Text.ToUpperInvariant() is not ("SELECT" or "WITH"))
            throw new ArgumentException("Native read-only capture supports SELECT and WITH statements only.", nameof(query));
        var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int index = first; index <= last; index++)
        {
            var token = tokens[index];
            if (token.Kind != SqlTokenKind.Parameter) continue;
            // @@system variables are native SQL, not bound placeholders.
            if (token.Text[0] == '@' && index > first && tokens[index - 1].Text == "@"
                && tokens[index - 1].Position + 1 == token.Position) continue;
            if (token.Text[0] is not ('@' or '?') || token.Text.Length == 1)
                throw new ArgumentException("Use named @parameters or ?parameters for bound estimated plans.", nameof(query));
            required.Add(token.Text.Substring(1));
        }
        var values = parameters == null ? null : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (parameters != null)
            foreach (var pair in parameters)
            {
                var name = pair.Key.TrimStart('@', '?');
                if (!required.Contains(name) || values!.ContainsKey(name))
                    throw new ArgumentException("Every value must name one used, unambiguous parameter.", nameof(parameters));
                values.Add(name, pair.Value);
            }
        if (required.Any(name => values == null || !values.ContainsKey(name)))
            throw new ArgumentException("Every named placeholder needs a bound value.", nameof(parameters));
        var types = parameterTypes == null ? null : new Dictionary<string, MySqlDbType>(StringComparer.OrdinalIgnoreCase);
        if (parameterTypes != null)
            foreach (var pair in parameterTypes)
            {
                var name = pair.Key.TrimStart('@', '?');
                if (values == null || !values.ContainsKey(name) || types!.ContainsKey(name) || !Enum.IsDefined(typeof(MySqlDbType), pair.Value))
                    throw new ArgumentException("Every valid provider type must name one bound parameter.", nameof(parameterTypes));
                types.Add(name, pair.Value);
            }
        return (query.Substring(tokens[first].Position, tokens[last].Position + tokens[last].Text.Length - tokens[first].Position), values, types);
    }
}
