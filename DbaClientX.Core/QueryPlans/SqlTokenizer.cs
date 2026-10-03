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
    Symbol,
    OpenBracket,
    CloseBracket
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
    /// <param name="sql">The SQL text.</param>
    /// <param name="dollarQuotes">Whether <c>$$ … $$</c> and <c>$tag$ … $tag$</c> are strings, as in PostgreSQL; in SQLite they are parameter names.</param>
    /// <param name="backslashStrings">Whether ordinary strings use MySQL's default backslash escapes.</param>
    /// <param name="sqliteParameters">Whether named parameters use SQLite's identifier, namespace and parenthesized suffix syntax.</param>
    /// <param name="nestedBlockComments">Whether block comments can nest, as in SQL Server.</param>
    /// <param name="bracketIdentifiers">Whether square brackets quote identifiers; false for PostgreSQL array syntax.</param>
    internal static IReadOnlyList<SqlToken> Tokenize(string sql, bool dollarQuotes = false, bool backslashStrings = false,
        bool sqliteParameters = false, bool nestedBlockComments = false, bool bracketIdentifiers = true)
        => Tokenize(sql, out _, dollarQuotes, backslashStrings, sqliteParameters, nestedBlockComments, bracketIdentifiers);

    internal static IReadOnlyList<SqlToken> Tokenize(string sql, out bool hasExecutableComments,
        bool dollarQuotes = false, bool backslashStrings = false, bool sqliteParameters = false, bool nestedBlockComments = false,
        bool bracketIdentifiers = true)
    {
        hasExecutableComments = false;
        var tokens = new List<SqlToken>();
        var index = 0;
        while (index < sql.Length)
        {
            var character = sql[index];
            if (char.IsWhiteSpace(character) || character == '\uFEFF')
            {
                index++;
            }
            else if (character == '-' && Next(sql, index) == '-' &&
                (!backslashStrings || index + 2 < sql.Length && (char.IsWhiteSpace(sql[index + 2]) || char.IsControl(sql[index + 2])))
                || backslashStrings && character == '#')
            {
                while (index < sql.Length && sql[index] != '\n')
                {
                    index++;
                }
            }
            else if (character == '/' && Next(sql, index) == '*')
            {
                hasExecutableComments |= index + 2 < sql.Length && (sql[index + 2] == '!' ||
                    index + 3 < sql.Length && sql[index + 2] is 'M' or 'm' && sql[index + 3] == '!');
                index = SkipBlockComment(sql, index, nestedBlockComments);
            }
            else if (character == '\'' || ((character is 'N' or 'n' or 'E' or 'e') && Next(sql, index) == '\''))
            {
                var start = index;
                var escapes = backslashStrings || character is 'E' or 'e';
                if (character != '\'')
                {
                    index++;
                }

                var value = ReadQuoted(sql, ref index, '\'', backslashEscapes: escapes, mysqlEscapes: backslashStrings);
                tokens.Add(new SqlToken(SqlTokenKind.String, sql.Substring(start, index - start), value, start));
            }
            else if (character is '"' or '`' || character == '[' && bracketIdentifiers)
            {
                var start = index;
                var value = ReadQuoted(sql, ref index, character == '[' ? ']' : character,
                    backslashEscapes: backslashStrings && character == '"', mysqlEscapes: backslashStrings);
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
                if (backslashStrings)
                {
                    // MySQL identifiers can begin with a digit. Keep numeric literals (including exponent,
                    // hex and bit forms) numeric, but consume an identifier and its qualifier separately.
                    int end = index;
                    while (end < sql.Length && (char.IsLetterOrDigit(sql[end]) || sql[end] is '_' or '$')) end++;
                    if (!IsMySqlNumericWord(sql, start, end))
                    {
                        index = end;
                        string identifier = sql.Substring(start, end - start);
                        tokens.Add(new SqlToken(SqlTokenKind.Word, identifier, identifier, start));
                        continue;
                    }
                }
                while (index < sql.Length && (char.IsLetterOrDigit(sql[index]) || sql[index] == '.'))
                {
                    index++;
                }

                var text = sql.Substring(start, index - start);
                tokens.Add(new SqlToken(SqlTokenKind.Number, text, text, start));
            }
            else if (dollarQuotes && character == '$' && DollarQuoteTag(sql, index) is { } tag)
            {
                // PostgreSQL dollar quoting ($$ body $$, $fn$ body $fn$): a string that may hold semicolons and quotes.
                var start = index;
                var close = sql.IndexOf(tag, start + tag.Length, System.StringComparison.Ordinal);
                index = close < 0 ? sql.Length : close + tag.Length;
                var bodyEnd = close < 0 ? sql.Length : close;
                tokens.Add(new SqlToken(SqlTokenKind.String, sql.Substring(start, index - start), sql.Substring(start + tag.Length, bodyEnd - start - tag.Length), start));
            }
            else if (sqliteParameters && character is '@' or ':' or '$' or '#' or '?')
            {
                var start = index;
                ReadSqliteParameter(sql, ref index);
                var text = sql.Substring(start, index - start);
                tokens.Add(new SqlToken(SqlTokenKind.Parameter, text, text, start));
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
                    '[' => SqlTokenKind.OpenBracket,
                    ']' => SqlTokenKind.CloseBracket,
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

    // SQLite's Tcl-style named parameters may contain :: and a non-whitespace suffix in parentheses.
    // Quotes, comment markers and semicolons in that suffix belong to the parameter, not to SQL syntax.
    private static void ReadSqliteParameter(string sql, ref int index)
    {
        if (sql[index++] == '?')
        {
            while (index < sql.Length && sql[index] is >= '0' and <= '9') index++;
            return;
        }

        var hasName = false;
        while (index < sql.Length)
        {
            var character = sql[index];
            if (IsSqliteIdentifierCharacter(character))
            {
                hasName = true;
                index++;
            }
            else if (character == ':' && Next(sql, index) == ':')
                index += 2;
            else if (character == '(' && hasName)
            {
                index++;
                while (index < sql.Length && sql[index] != '\0' &&
                       sql[index] != ')' && !IsSqliteSpace(sql[index])) index++;
                if (index < sql.Length && sql[index] == ')') index++;
                return;
            }
            else return;
        }
    }

    private static bool IsSqliteIdentifierCharacter(char character)
        => character >= '\u0080' || character is >= 'a' and <= 'z' or >= 'A' and <= 'Z'
            or >= '0' and <= '9' or '_' or '$';

    private static bool IsSqliteSpace(char character) => character is ' ' or '\t' or '\n' or '\f' or '\r';
    private static int SkipBlockComment(string sql, int start, bool nested)
    {
        int depth = 1;
        for (int index = start + 2; index + 1 < sql.Length; index++)
        {
            if (nested && sql[index] == '/' && sql[index + 1] == '*') { depth++; index++; }
            else if (sql[index] == '*' && sql[index + 1] == '/')
            {
                if (--depth == 0) return index + 2;
                index++;
            }
        }
        return sql.Length;
    }

    private static bool IsMySqlNumericWord(string sql, int start, int end)
    {
        int index = start;
        while (index < end && char.IsDigit(sql[index])) index++;
        if (index == end) return true;
        if (end > start + 2 && sql[start] == '0' && sql[start + 1] is 'x' or 'X' or 'b' or 'B')
        {
            bool binary = sql[start + 1] is 'b' or 'B';
            for (int digit = start + 2; digit < end; digit++)
                if (binary ? sql[digit] is not ('0' or '1') :
                    !(sql[digit] is >= '0' and <= '9' or >= 'a' and <= 'f' or >= 'A' and <= 'F')) return false;
            return true;
        }
        if (sql[index] is not ('e' or 'E')) return false;
        index++;
        if (index == end)
            return end + 1 < sql.Length && sql[end] is '+' or '-' && char.IsDigit(sql[end + 1]);
        for (; index < end; index++) if (!char.IsDigit(sql[index])) return false;
        return true;
    }

    /// <summary>The opening tag of a dollar-quoted string at <paramref name="index"/> (<c>$$</c> or <c>$tag$</c>), or null for a <c>$1</c> or <c>$name</c> parameter.</summary>
    private static string? DollarQuoteTag(string sql, int index)
    {
        var end = index + 1;
        if (end < sql.Length && char.IsDigit(sql[end]))
        {
            return null;
        }

        while (end < sql.Length && (char.IsLetterOrDigit(sql[end]) || sql[end] == '_'))
        {
            end++;
        }

        return end < sql.Length && sql[end] == '$' ? sql.Substring(index, end + 1 - index) : null;
    }

    /// <summary>Reads a quoted run starting at the opening quote; a doubled closing quote stands for itself.</summary>
    private static string ReadQuoted(string sql, ref int index, char close, bool backslashEscapes, bool mysqlEscapes = false)
    {
        var value = new StringBuilder();
        index++;
        while (index < sql.Length)
        {
            var character = sql[index];
            if (backslashEscapes && character == '\\' && index + 1 < sql.Length)
            {
                char escaped = sql[index + 1];
                if (mysqlEscapes && escaped is '%' or '_') value.Append('\\');
                value.Append(mysqlEscapes ? escaped switch
                {
                    '0' => '\0', 'b' => '\b', 'n' => '\n', 'r' => '\r', 't' => '\t', 'Z' => (char)26,
                    _ => escaped
                } : escaped);
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
