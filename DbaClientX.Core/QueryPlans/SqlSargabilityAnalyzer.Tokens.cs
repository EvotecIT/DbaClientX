using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

public static partial class SqlSargabilityAnalyzer
{
    private static bool IsName(SqlToken token) => token.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier or SqlTokenKind.String;

    private static bool IsWord(SqlToken token, string word)
        => token.Kind == SqlTokenKind.Word && string.Equals(token.Text, word, StringComparison.OrdinalIgnoreCase);

    private static int FindOpeningParenthesis(IReadOnlyList<SqlToken> tokens, int close)
    {
        var depth = 0;
        for (var index = close; index >= 0; index--)
        {
            if (tokens[index].Kind == SqlTokenKind.CloseParenthesis)
            {
                depth++;
            }
            else if (tokens[index].Kind == SqlTokenKind.OpenParenthesis && --depth == 0)
            {
                return index;
            }
        }

        return -1;
    }

    private static int FindClosingParenthesis(IReadOnlyList<SqlToken> tokens, int open)
    {
        var depth = 0;
        for (var index = open; index < tokens.Count; index++)
        {
            if (tokens[index].Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if (tokens[index].Kind == SqlTokenKind.CloseParenthesis && --depth == 0)
            {
                return index;
            }
        }

        return -1;
    }
}
