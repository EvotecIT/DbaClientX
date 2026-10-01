using System.Collections.Generic;
using System.Text;

namespace DBAClientX.QueryPlans;

internal enum SqlTokenKind
{
    Word,
    QuotedIdentifier,
    String,
    Number,
    Parameter,
    OpenParenthesis,
    CloseParenthesis,
    Semicolon,
    Symbol
}

/// <summary>A token of SQL text: its kind, its text as written, its value (unquoted strings) and its offset.</summary>
internal readonly struct SqlToken
{
    internal SqlToken(SqlTokenKind kind, string text, string value, int position)
    {
        Kind = kind;
        Text = text;
        Value = value;
        Position = position;
    }

    internal SqlTokenKind Kind { get; }

    internal string Text { get; }

    internal string Value { get; }

    internal int Position { get; }
}

/// <summary>
/// Splits SQL text into tokens for heuristics, skipping comments and keeping string literals and quoted identifiers
/// whole (doubled quotes inside them, N'' and E'' prefixes). It does not validate SQL.
/// </summary>
internal static class SqlTokenizer
{
    internal static IReadOnlyList<SqlToken> Tokenize(string sql)
    {
        var tokens = new List<SqlToken>();
        var index = 0;
        while (index < sql.Length)
        {
            var character = sql[index];
            if (char.IsWhiteSpace(character))
            {
                index++;
            }
            else if (character == '-' && Next(sql, index) == '-')
            {
                while (index < sql.Length && sql[index] != '\n')
                {
                    index++;
                }
            }
            else if (character == '/' && Next(sql, index) == '*')
            {
                var end = sql.IndexOf("*/", index + 2, System.StringComparison.Ordinal);
                index = end < 0 ? sql.Length : end + 2;
            }
            else if (character == '\'' || ((character is 'N' or 'n' or 'E' or 'e') && Next(sql, index) == '\''))
            {
                var start = index;
                var escapes = character is 'E' or 'e';
                if (character != '\'')
                {
                    index++;
                }

                var value = ReadQuoted(sql, ref index, '\'', backslashEscapes: escapes);
                tokens.Add(new SqlToken(SqlTokenKind.String, sql.Substring(start, index - start), value, start));
            }
            else if (character is '"' or '`' or '[')
            {
                var start = index;
                var value = ReadQuoted(sql, ref index, character == '[' ? ']' : character, backslashEscapes: false);
                tokens.Add(new SqlToken(SqlTokenKind.QuotedIdentifier, sql.Substring(start, index - start), value, start));
            }
            else if (char.IsLetter(character) || character == '_')
            {
                var start = index;
                while (index < sql.Length && (char.IsLetterOrDigit(sql[index]) || sql[index] is '_' or '$'))
                {
                    index++;
                }

                var text = sql.Substring(start, index - start);
                tokens.Add(new SqlToken(SqlTokenKind.Word, text, text, start));
            }
            else if (char.IsDigit(character))
            {
                var start = index;
                while (index < sql.Length && (char.IsLetterOrDigit(sql[index]) || sql[index] == '.'))
                {
                    index++;
                }

                var text = sql.Substring(start, index - start);
                tokens.Add(new SqlToken(SqlTokenKind.Number, text, text, start));
            }
            else if (character == ':' && Next(sql, index) == ':')
            {
                // PostgreSQL cast operator.
                tokens.Add(new SqlToken(SqlTokenKind.Symbol, "::", "::", index));
                index += 2;
            }
            else if (character is '@' or ':' or '$' or '?' && (character == '?' || char.IsLetterOrDigit(Next(sql, index)) || Next(sql, index) == '_'))
            {
                var start = index++;
                while (index < sql.Length && (char.IsLetterOrDigit(sql[index]) || sql[index] == '_'))
                {
                    index++;
                }

                var text = sql.Substring(start, index - start);
                tokens.Add(new SqlToken(SqlTokenKind.Parameter, text, text, start));
            }
            else
            {
                var kind = character switch
                {
                    '(' => SqlTokenKind.OpenParenthesis,
                    ')' => SqlTokenKind.CloseParenthesis,
                    ';' => SqlTokenKind.Semicolon,
                    _ => SqlTokenKind.Symbol
                };
                tokens.Add(new SqlToken(kind, character.ToString(), character.ToString(), index));
                index++;
            }
        }

        return tokens;
    }

    private static char Next(string sql, int index) => index + 1 < sql.Length ? sql[index + 1] : '\0';

    /// <summary>Reads a quoted run starting at the opening quote; a doubled closing quote stands for itself.</summary>
    private static string ReadQuoted(string sql, ref int index, char close, bool backslashEscapes)
    {
        var value = new StringBuilder();
        index++;
        while (index < sql.Length)
        {
            var character = sql[index];
            if (backslashEscapes && character == '\\' && index + 1 < sql.Length)
            {
                value.Append(sql[index + 1]);
                index += 2;
                continue;
            }

            if (character == close)
            {
                if (Next(sql, index) == close)
                {
                    value.Append(close);
                    index += 2;
                    continue;
                }

                index++;
                return value.ToString();
            }

            value.Append(character);
            index++;
        }

        return value.ToString();
    }
}
