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
        var placeholders = new List<string>();
        var statementEnd = tokens[last].Position + tokens[last].Text.Length;
        for (int index = first; index <= last; index++)
        {
            var token = tokens[index];
            if (token.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Symbol)) continue;
            if (token.Text[0] == ':') throw new ArgumentException("Use native named @parameters or ?parameters.", nameof(query));
            if (token.Text[0] is not ('@' or '?')) continue;
            // @@system variables are native SQL, not bound placeholders.
            if (token.Text[0] == '@' && index > first && tokens[index - 1].Text == "@"
                && tokens[index - 1].Position + 1 == token.Position) continue;
            var end = token.Position + 1;
            if (token.Text[0] == '@' && end < query.Length && query[end] == '@') continue;
            if (token.Text[0] == '@' && index < last && tokens[index + 1].Position == end
                && tokens[index + 1].Kind is SqlTokenKind.QuotedIdentifier or SqlTokenKind.String)
            {
                index++;
                end = tokens[index].Position + tokens[index].Text.Length;
            }
            else
            {
                while (end < query.Length && IsNativePlanParameterCharacter(query[end])) end++;
                if (end == token.Position + 1)
                    throw new ArgumentException("Use named @parameters or ?parameters; positional placeholders are unsupported.", nameof(query));
                while (index < last && tokens[index + 1].Position < end) index++;
            }
            placeholders.Add(query.Substring(token.Position, end - token.Position));
            statementEnd = Math.Max(statementEnd, end);
        }
        // Let the existing provider normalize quoted names, prefixes and case, rather than duplicating its binding rules.
        using var binding = new MySqlCommand();
        var values = parameters == null ? null : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (parameters != null)
            foreach (var pair in parameters)
            {
                if (binding.Parameters.IndexOf(pair.Key) >= 0)
                    throw new ArgumentException("Every value must name one used, unambiguous parameter.", nameof(parameters));
                binding.Parameters.Add(new MySqlParameter(pair.Key, pair.Value ?? DBNull.Value));
                values!.Add(pair.Key, pair.Value);
            }
        var used = new HashSet<int>();
        foreach (var placeholder in placeholders)
        {
            var index = binding.Parameters.IndexOf(placeholder);
            if (index < 0) throw new ArgumentException("Every named placeholder needs a bound value.", nameof(parameters));
            used.Add(index);
        }
        if (used.Count != binding.Parameters.Count)
            throw new ArgumentException("Every value must name one used parameter.", nameof(parameters));
        var types = parameterTypes == null ? null : new Dictionary<string, MySqlDbType>(StringComparer.OrdinalIgnoreCase);
        if (parameterTypes != null)
            foreach (var pair in parameterTypes)
            {
                var index = binding.Parameters.IndexOf(pair.Key);
                if (index < 0 || !Enum.IsDefined(typeof(MySqlDbType), pair.Value))
                    throw new ArgumentException("Every valid provider type must name one bound parameter.", nameof(parameterTypes));
                var name = binding.Parameters[index].ParameterName;
                if (types!.ContainsKey(name)) throw new ArgumentException("Every type must name one unambiguous bound parameter.", nameof(parameterTypes));
                types.Add(name, pair.Value);
            }
        return (query.Substring(tokens[first].Position, statementEnd - tokens[first].Position), values, types);
    }

    // MySqlConnector's native unquoted variable grammar also permits dots, dollars and non-ASCII characters.
    private static bool IsNativePlanParameterCharacter(char character)
        => character is >= 'a' and <= 'z' or >= 'A' and <= 'Z' or >= '0' and <= '9' or '_' or '.' or '$' or >= '\u0080';
}
