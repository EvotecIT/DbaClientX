using System.Text;
using DBAClientX.QueryBuilder;
using DBAClientX.QueryPlans;

namespace DBAClientX;

public partial class SqlServer
{
    private static (string Batch, DbaQueryPlanParameterMode Mode) BuildQueryPlanBatch(
        string query, IDictionary<string, SqlServerQueryPlanParameter>? parameterTypes)
    {
        ValidateCommandText(query);
        if (query.Length > 1024 * 1024) throw new ArgumentException("An explained statement cannot exceed one million characters.", nameof(query));
        var statements = SqlStatementText.Split(query, SqlDialect.SqlServer);
        if (statements.Count != 1) throw new ArgumentException("Explain one statement at a time.", nameof(query));
        var tokens = SqlTokenizer.Tokenize(statements[0], nestedBlockComments: true);
        if (tokens.Count == 0 || tokens[0].Kind != SqlTokenKind.Word
            || !new[] { "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE" }.Contains(tokens[0].Value, StringComparer.OrdinalIgnoreCase))
            throw new ArgumentException("Explain a SELECT, INSERT, UPDATE, DELETE or MERGE statement; session commands and procedural batches are unsupported.", nameof(query));
        // SHOWPLAN remains controlled exclusively by this operation, in its own batches. Do not interpret quoted names/literals as controls.
        foreach (var token in tokens)
            if (token.Kind == SqlTokenKind.Word && new[] { "SHOWPLAN_XML", "SHOWPLAN_ALL", "SHOWPLAN_TEXT", "EXEC", "EXECUTE", "DECLARE", "GO" }
                .Contains(token.Value, StringComparer.OrdinalIgnoreCase))
                throw new ArgumentException("Session controls, dynamic execution and declarations cannot be explained by this API.", nameof(query));

        var declarations = new Dictionary<string, SqlServerQueryPlanParameter>(StringComparer.Ordinal);
        if (parameterTypes != null)
        {
            if (parameterTypes.Count > 2100) throw new ArgumentException("At most 2100 parameter declarations are supported.", nameof(parameterTypes));
            foreach (var pair in parameterTypes)
            {
                string name = pair.Key;
                if (string.IsNullOrEmpty(name) || name.Length > 128 || name[0] != '@' || name.Length < 2
                    || !(IsPlanNameLetter(name[1]) || name[1] == '_') || name.Skip(2).Any(character => !IsPlanNameLetter(character) && character != '_' && !(character >= '0' && character <= '9')))
                    throw new ArgumentException("Parameter names must start with @ and use ASCII letters, digits and underscores.", nameof(parameterTypes));
                if (pair.Value == null || declarations.ContainsKey(name)) throw new ArgumentException("Parameter declarations must be non-null and unique.", nameof(parameterTypes));
                declarations.Add(name, pair.Value);
            }
        }
        var used = new HashSet<string>(StringComparer.Ordinal);
        foreach (var token in tokens)
        {
            if (token.Kind != SqlTokenKind.Parameter) continue;
            // @@system variables are tokenized as a preceding @ symbol followed by a parameter.
            if (token.Position > 0 && statements[0][token.Position - 1] == '@') continue;
            if (!declarations.ContainsKey(token.Text)) throw new ArgumentException("Supply the scalar type of every explained parameter using its exact spelling. Values are not bound or interpolated.", nameof(parameterTypes));
            used.Add(token.Text);
        }
        if (used.Count != declarations.Count) throw new ArgumentException("Every parameter declaration must be used by the statement.", nameof(parameterTypes));
        var batch = new StringBuilder();
        foreach (var pair in declarations) batch.Append("DECLARE ").Append(pair.Key).Append(' ').Append(pair.Value.Declaration).AppendLine(";");
        batch.Append(statements[0]);
        return (batch.ToString(), declarations.Count == 0 ? DbaQueryPlanParameterMode.None : DbaQueryPlanParameterMode.TypedVariables);
    }

    private static bool IsPlanNameLetter(char character) => character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z';
}
