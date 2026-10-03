using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

internal sealed partial class SqlSourceScopes
{
    private static bool IsIndexHintAt(IReadOnlyList<SqlToken> tokens, int position)
        => position + 1 < tokens.Count &&
            (IsWord(tokens[position], "USE") || IsWord(tokens[position], "FORCE") || IsWord(tokens[position], "IGNORE")) &&
            (IsWord(tokens[position + 1], "INDEX") || IsWord(tokens[position + 1], "KEY"));

    private static int SkipTemporalPeriod(IReadOnlyList<SqlToken> tokens, int position, ISet<int>? sourceModifiers)
    {
        if (position + 2 >= tokens.Count || !IsWord(tokens[position], "FOR") || !IsWord(tokens[position + 1], "SYSTEM_TIME"))
            return position;
        int start = position;
        position += 2;
        if (IsWord(tokens[position], "ALL"))
        {
            MarkRange(sourceModifiers, start, position + 1);
            return position + 1;
        }
        if (IsWord(tokens[position], "AS") && position + 1 < tokens.Count && IsWord(tokens[position + 1], "OF"))
        {
            MarkRange(sourceModifiers, start, position + 2);
            return SkipTemporalPoint(tokens, position + 2, sourceModifiers);
        }
        bool between = IsWord(tokens[position], "BETWEEN");
        if (between || IsWord(tokens[position], "FROM"))
        {
            MarkRange(sourceModifiers, start, position + 1);
            position = SkipTemporalPoint(tokens, position + 1, sourceModifiers);
            if (position < tokens.Count && IsWord(tokens[position], between ? "AND" : "TO"))
            {
                sourceModifiers?.Add(position);
                return SkipTemporalPoint(tokens, position + 1, sourceModifiers);
            }
            return position;
        }
        return start;
    }

    private static int SkipTemporalPoint(IReadOnlyList<SqlToken> tokens, int position, ISet<int>? sourceModifiers)
    {
        if (position + 1 < tokens.Count && (IsWord(tokens[position], "TRANSACTION") ||
            IsWord(tokens[position], "TIMESTAMP") && tokens[position + 1].Kind != SqlTokenKind.OpenParenthesis))
        {
            sourceModifiers?.Add(position++);
        }
        position = SkipTemporalAtom(tokens, position);
        while (position < tokens.Count && IsTemporalOperator(tokens[position]))
        {
            int next = SkipTemporalAtom(tokens, position + 1);
            if (next == position + 1) break;
            position = next;
        }
        return position;
    }

    private static int SkipTemporalAtom(IReadOnlyList<SqlToken> tokens, int position)
    {
        while (position < tokens.Count && tokens[position].Text is "+" or "-" or "~") position++;
        if (position >= tokens.Count) return position;
        if (tokens[position].Kind == SqlTokenKind.OpenParenthesis) return SkipParentheses(tokens, position);
        if (IsWord(tokens[position], "CASE"))
        {
            int cases = 1;
            for (position++; position < tokens.Count; position++)
            {
                if (IsWord(tokens[position], "CASE")) cases++;
                else if (IsWord(tokens[position], "END") && --cases == 0) return position + 1;
            }
            return position;
        }
        if (IsWord(tokens[position], "INTERVAL"))
        {
            position = SkipTemporalAtom(tokens, position + 1);
            return position < tokens.Count && IsName(tokens[position]) ? position + 1 : position;
        }
        if (position + 1 < tokens.Count && tokens[position + 1].Kind == SqlTokenKind.String &&
            (IsWord(tokens[position], "DATE") || IsWord(tokens[position], "TIME") || IsWord(tokens[position], "TIMESTAMP")))
            return position + 2;
        if (!IsName(tokens[position]) && tokens[position].Kind is not (SqlTokenKind.String or SqlTokenKind.Number or SqlTokenKind.Parameter))
            return position;
        position++;
        while (position + 1 < tokens.Count && tokens[position].Text == "." && IsName(tokens[position + 1])) position += 2;
        return position < tokens.Count && tokens[position].Kind == SqlTokenKind.OpenParenthesis
            ? SkipParentheses(tokens, position) : position;
    }

    private static bool IsTemporalOperator(SqlToken token)
        => token.Text is "+" or "-" or "*" or "/" or "%" or "|" or "&" or "^" or "<<" or ">>" ||
            IsWord(token, "DIV") || IsWord(token, "MOD") || IsWord(token, "COLLATE");
}
