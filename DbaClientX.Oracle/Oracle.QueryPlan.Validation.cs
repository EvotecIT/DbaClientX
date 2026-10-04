using DBAClientX.QueryPlans;
using Oracle.ManagedDataAccess.Client;
using System.Text.RegularExpressions;

namespace DBAClientX;

public partial class Oracle
{
    private static (string Statement, Dictionary<string, object?>? Values, Dictionary<string, OracleDbType>? Types)
        ValidateQueryPlanInput(string query, IDictionary<string, object?>? parameters, IDictionary<string, OracleDbType>? parameterTypes)
    {
        if (string.IsNullOrWhiteSpace(query) || query.Length > 1024 * 1024)
            throw new ArgumentException("One SELECT/WITH statement of at most 1,048,576 characters is required.", nameof(query));
        var tokens = SqlTokenizer.Tokenize(query, bracketIdentifiers: false, oracleAlternativeQuotes: true, oracleIdentifiers: true);
        int last = tokens.Count - 1;
        if (last >= 0 && tokens[last].Kind == SqlTokenKind.Semicolon) last--;
        if (last < 0 || tokens.Take(last + 1).Any(token => token.Kind == SqlTokenKind.Semicolon)
            || tokens[0].Kind != SqlTokenKind.Word || tokens[0].Text.ToUpperInvariant() is not ("SELECT" or "WITH"))
            throw new ArgumentException("Capture supports one SELECT/WITH statement, without scripts or PL/SQL.", nameof(query));
        var required = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        int depth = 0;
        bool select = false;
        for (int index = 0; index <= last; index++)
        {
            var token = tokens[index];
            if (token.Kind == SqlTokenKind.OpenParenthesis) depth++;
            if (token.Kind == SqlTokenKind.CloseParenthesis && --depth < 0) throw new ArgumentException("Unbalanced statement.", nameof(query));
            if (depth == 0 && token.Kind == SqlTokenKind.Word && (index == 0 || tokens[index - 1].Text != "."))
            {
                string word = token.Text.ToUpperInvariant();
                if (word is "INSERT" or "UPDATE" or "DELETE" or "MERGE" or "FUNCTION" or "PROCEDURE")
                    throw new ArgumentException("Write statements and local PL/SQL functions are unsupported.", nameof(query));
                if (word == "SELECT") select = true;
            }
            if (token.Kind is not (SqlTokenKind.Parameter or SqlTokenKind.Symbol)) continue;
            if (token.Text[0] == '?') throw new ArgumentException("Use native named :parameters.", nameof(query));
            // @ belongs to Oracle database links. It is never a bind marker in this provider; native SQL validates it.
            if (token.Text[0] != ':') continue;
            int end = token.Position + 1;
            while (end < query.Length && IsQueryPlanNameCharacter(query[end])) end++;
            if (end == token.Position + 1 || !char.IsLetter(query[token.Position + 1]))
                throw new ArgumentException("Use unquoted named :parameters beginning with a letter.", nameof(query));
            required.Add(query.Substring(token.Position + 1, end - token.Position - 1));
            while (index < last && tokens[index + 1].Position < end) index++;
        }
        if (depth != 0 || !select) throw new ArgumentException("A complete SELECT/WITH statement is required.", nameof(query));
        string Name(string name)
        {
            string result = name.StartsWith(":", StringComparison.Ordinal) ? name.Substring(1) : name;
            if (result.Length == 0 || !char.IsLetter(result[0]) || result.Any(character => !IsQueryPlanNameCharacter(character)))
                throw new ArgumentException("Parameter names must be unquoted Oracle identifiers.", nameof(parameters));
            return result;
        }
        var values = parameters == null ? null : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        if (parameters != null)
            foreach (var pair in parameters)
            {
                string name = Name(pair.Key);
                if (!required.Contains(name) || values!.ContainsKey(name)) throw new ArgumentException("Every value must name one used, unambiguous parameter.", nameof(parameters));
                values.Add(name, pair.Value);
            }
        if (required.Any(name => values == null || !values.ContainsKey(name))) throw new ArgumentException("Every placeholder needs a bound value.", nameof(parameters));
        var types = parameterTypes == null ? null : new Dictionary<string, OracleDbType>(StringComparer.OrdinalIgnoreCase);
        if (parameterTypes != null)
            foreach (var pair in parameterTypes)
            {
                string name = Name(pair.Key);
                if (values == null || !values.ContainsKey(name) || types!.ContainsKey(name)) throw new ArgumentException("Every type must name one bound parameter.", nameof(parameterTypes));
                types.Add(name, pair.Value);
            }
        int start = tokens[0].Position, endPosition = tokens[last].Position + tokens[last].Text.Length;
        // Drop comments after the final significant token; comments inside the statement retain their native text.
        return (query.Substring(start, endPosition - start), values, types);
    }

    private static bool IsQueryPlanNameCharacter(char character) => char.IsLetterOrDigit(character) || character is '_' or '$' or '#';

    private static void ValidateQueryPlanTransport(string dataSource)
    {
        if (dataSource.StartsWith("tcps://", StringComparison.OrdinalIgnoreCase)) return;
        if (!dataSource.TrimStart().StartsWith("(", StringComparison.Ordinal))
            throw new ArgumentException("Plan capture requires explicit TCPS Easy Connect or a TCPS-only descriptor; unresolved aliases are unsupported.", "connectionString");
        // Ignore quoted descriptor values so text in HOST/DN cannot impersonate an address protocol.
        string unquoted = Regex.Replace(dataSource, "\"(?:\"\"|[^\"])*\"", "\"\"", RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        var protocols = Regex.Matches(unquoted, @"\(\s*PROTOCOL\s*=\s*([^()]*)\)", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));
        if (protocols.Count == 0 || protocols.Cast<Match>().Any(match => !match.Groups[1].Value.Trim().Equals("TCPS", StringComparison.OrdinalIgnoreCase)))
            throw new ArgumentException("All plan-capture addresses must explicitly use TCPS.", "connectionString");
    }
}
