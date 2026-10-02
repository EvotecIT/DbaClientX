using System;
using System.Collections.Generic;

namespace DBAClientX.QueryPlans;

/// <summary>A query that looks up the smallest or largest value of one column (<c>SELECT MAX(Seen) FROM t …</c>).</summary>
/// <param name="Table">The table read, without a schema prefix.</param>
/// <param name="Column">The column, unqualified.</param>
/// <param name="WhereTerms">The <c>AND</c>-joined terms of its <c>WHERE</c> as plans count them (see
/// <see cref="SqlQueryLevels.CountWhereTerms"/>), 0 without one, or -1 when they cannot be counted.</param>
/// <param name="Nested">Whether the lookup is inside parentheses (a subquery or CTE) rather than the statement itself.</param>
internal readonly record struct SqlMinMaxLookup(string Table, string Column, int WhereTerms, bool Nested);

/// <summary>
/// Reads the query levels of SQL text (the statement, and each parenthesized subquery) for plan rules that need to know
/// what a table's own query does: whether a <c>LIMIT</c> stops it, and whether it is a <c>MIN</c>/<c>MAX</c> lookup. A
/// heuristic over tokens, not a parser.
/// </summary>
internal static partial class SqlQueryLevels
{
    private static readonly HashSet<string> SourceKeywords = new(StringComparer.OrdinalIgnoreCase) { "FROM", "JOIN", "UPDATE", "INTO" };

    private static readonly HashSet<string> NotAliases = new(StringComparer.OrdinalIgnoreCase)
    {
        "WHERE", "JOIN", "LEFT", "RIGHT", "FULL", "INNER", "OUTER", "CROSS", "NATURAL", "ON", "USING", "GROUP", "ORDER",
        "LIMIT", "OFFSET", "UNION", "INTERSECT", "EXCEPT", "WINDOW", "HAVING", "SET", "VALUES", "RETURNING", "INDEXED",
        "NOT", "AS", "SELECT", "FROM", "WITH", "FETCH", "FOR", "OPTION", "DEFAULT", "OR"
    };

    // Aggregates read every row their query finds before a LIMIT applies to their result.
    private static readonly HashSet<string> Aggregates = new(StringComparer.OrdinalIgnoreCase)
    {
        "COUNT", "SUM", "AVG", "MIN", "MAX", "TOTAL", "GROUP_CONCAT", "STRING_AGG", "ARRAY_AGG", "JSON_GROUP_ARRAY",
        "JSON_GROUP_OBJECT", "JSONB_GROUP_ARRAY", "JSONB_GROUP_OBJECT", "MEDIAN", "STDEV", "VARIANCE"
    };

    /// <summary>
    /// Whether every query level of <paramref name="sql"/> that names <paramref name="table"/> (or its alias) as a source
    /// stops after a few rows: it has a <c>LIMIT</c> of its own (not <c>LIMIT -1</c>, nor one of a subquery inside it),
    /// a <c>FETCH FIRST</c>/<c>NEXT</c> or a <c>SELECT TOP</c>, or it is the subquery of an <c>EXISTS</c>; and it neither
    /// aggregates nor groups, which reads every row before the limit applies. Only the statement's own level counts for
    /// a top-level plan step, and only parenthesized levels (subqueries, CTEs) for a nested one. False when no such
    /// level names the table.
    /// </summary>
    /// <param name="sql">The statement.</param>
    /// <param name="table">The table the plan step reads.</param>
    /// <param name="nested">Whether the plan step is nested (under a subquery or co-routine).</param>
    /// <param name="indexColumns">The columns the step's index search constrains, with their operators. A level counts
    /// only when it reads one table and each term of its <c>WHERE</c> is served (see <see cref="ServesEveryCondition"/>):
    /// another condition can make the read go on far past the limit, looking for rows that pass it.</param>
    /// <param name="partialSort">Null when the rows come fully ordered; when they are sorted by their last <c>ORDER BY</c>
    /// terms as they come, tells whether one value of the index key up to a column holds few rows: the index must then
    /// read a range of the leading <c>ORDER BY</c> column, whose values hold few rows each.</param>
    /// <param name="maximumRows">The largest limited read considered narrow, when table statistics are available.</param>
    internal static bool HasLimitWhereRead(string sql, string table, bool nested, IReadOnlyList<(string Column, string Operator)>? indexColumns, Func<string, bool>? partialSort, double? maximumRows)
    {
        var tokens = SqlTokenizer.Tokenize(sql);
        var found = false;
        var depth = 0;
        for (var index = 0; index + 1 < tokens.Count; index++)
        {
            switch (tokens[index].Kind)
            {
                case SqlTokenKind.OpenParenthesis:
                    depth++;
                    continue;
                case SqlTokenKind.CloseParenthesis:
                    depth = Math.Max(0, depth - 1);
                    continue;
                case SqlTokenKind.Semicolon:
                    depth = 0;
                    continue;
            }

            if (tokens[index].Kind != SqlTokenKind.Word || !SourceKeywords.Contains(tokens[index].Text) || (depth > 0) != nested)
            {
                continue;
            }

            var name = LastNamePart(tokens, index + 1, out var after);
            if (name == null)
            {
                continue;
            }

            // The step names the table, or (for an alias defined for several tables) the alias.
            if (!string.Equals(name, table, StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(AliasAt(tokens, after), table, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            found = true;
            var level = LevelAround(tokens, index);
            if (indexColumns == null || !StopsEarly(tokens, level, maximumRows) || !ServesEveryCondition(tokens, level, indexColumns, partialSort))
            {
                return false;
            }
        }

        return found;
    }

    /// <summary>
    /// Finds the queries that look up the smallest or largest value of one column, which SQLite answers from one end of
    /// an index when one fits: <c>SELECT [COALESCE(|IFNULL(]MIN|MAX(column)[, literal)] [[AS] name] FROM table [[AS] alias]
    /// [WHERE …]</c> with no join or <c>GROUP BY</c>.
    /// </summary>
    internal static IReadOnlyList<SqlMinMaxLookup> FindMinMaxLookups(string sql)
    {
        var tokens = SqlTokenizer.Tokenize(sql);
        var lookups = new List<SqlMinMaxLookup>();
        var depth = 0;
        for (var index = 0; index < tokens.Count; index++)
        {
            switch (tokens[index].Kind)
            {
                case SqlTokenKind.OpenParenthesis:
                    depth++;
                    continue;
                case SqlTokenKind.CloseParenthesis:
                    depth = Math.Max(0, depth - 1);
                    continue;
                case SqlTokenKind.Semicolon:
                    depth = 0;
                    continue;
            }

            if (IsWord(tokens[index], "SELECT") && TryReadMinMax(tokens, index, depth > 0) is { } lookup)
            {
                lookups.Add(lookup);
            }
        }

        return lookups;
    }

    private static SqlMinMaxLookup? TryReadMinMax(IReadOnlyList<SqlToken> tokens, int select, bool nested)
    {
        var index = select + 1;
        if (index < tokens.Count && IsWord(tokens[index], "ALL"))
        {
            index++;
        }

        var wrapped = index + 1 < tokens.Count && (IsWord(tokens[index], "COALESCE") || IsWord(tokens[index], "IFNULL")) &&
                      tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis;
        if (wrapped)
        {
            index += 2;
        }

        if (index + 1 >= tokens.Count || !(IsWord(tokens[index], "MIN") || IsWord(tokens[index], "MAX")) ||
            tokens[index + 1].Kind != SqlTokenKind.OpenParenthesis)
        {
            return null;
        }

        var column = LastNamePart(tokens, index + 2, out index);
        if (column == null || index >= tokens.Count || tokens[index].Kind != SqlTokenKind.CloseParenthesis)
        {
            return null;
        }

        index++;
        if (wrapped)
        {
            // , literal )
            if (index >= tokens.Count || tokens[index].Text != ",")
            {
                return null;
            }

            index++;
            if (index < tokens.Count && tokens[index].Kind == SqlTokenKind.Symbol && tokens[index].Text is "-" or "+")
            {
                index++;
            }

            if (index + 1 >= tokens.Count || !(tokens[index].Kind is SqlTokenKind.Number or SqlTokenKind.String or SqlTokenKind.Parameter || IsWord(tokens[index], "NULL")) ||
                tokens[index + 1].Kind != SqlTokenKind.CloseParenthesis)
            {
                return null;
            }

            index += 2;
        }

        if (index < tokens.Count && IsWord(tokens[index], "AS"))
        {
            index++;
        }

        if (index < tokens.Count && IsName(tokens[index]) && !IsWord(tokens[index], "FROM"))
        {
            index++;
        }

        if (index >= tokens.Count || !IsWord(tokens[index], "FROM"))
        {
            return null;
        }

        var table = LastNamePart(tokens, index + 1, out index);
        if (table == null)
        {
            return null;
        }

        if (AliasAt(tokens, index) != null)
        {
            index += index < tokens.Count && IsWord(tokens[index], "AS") ? 2 : 1;
        }

        var whereTerms = 0;
        if (index < tokens.Count && IsWord(tokens[index], "WHERE"))
        {
            whereTerms = CountWhereTerms(tokens, index + 1, out index);
        }

        var ends = index >= tokens.Count || tokens[index].Kind is SqlTokenKind.CloseParenthesis or SqlTokenKind.Semicolon ||
                   IsWord(tokens[index], "LIMIT") || IsWord(tokens[index], "UNION") || IsWord(tokens[index], "EXCEPT") ||
                   IsWord(tokens[index], "INTERSECT");
        return ends ? new SqlMinMaxLookup(table, column, whereTerms, nested) : null;
    }

    /// <summary>
    /// Counts the <c>AND</c>-joined terms of a <c>WHERE</c> starting at <paramref name="index"/> up to the end of its
    /// level, as SQLite's plan lists the conditions an index serves: a <c>BETWEEN</c>, <c>LIKE</c> or <c>GLOB</c> counts as
    /// two (a prefix pattern becomes a range). Returns -1 when the terms cannot be matched that way: an <c>OR</c> or
    /// <c>NOT</c> joins them at the top, or an <c>IN</c> holds several values or a subquery (the plan prints it as one
    /// equality, but it reads each value's rows).
    /// </summary>
    internal static int CountWhereTerms(IReadOnlyList<SqlToken> tokens, int index, out int end)
    {
        var terms = 1;
        var depth = 0;
        var between = false;
        var countable = true;
        for (; index < tokens.Count; index++)
        {
            var token = tokens[index];
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                if (depth == 0)
                {
                    break;
                }

                depth--;
                continue;
            }

            if (depth > 0)
            {
                continue;
            }

            if (token.Kind == SqlTokenKind.Semicolon || IsWord(token, "LIMIT") || IsWord(token, "ORDER") || IsWord(token, "GROUP") ||
                IsWord(token, "HAVING") || IsWord(token, "UNION") || IsWord(token, "EXCEPT") || IsWord(token, "INTERSECT") ||
                IsWord(token, "WINDOW") || IsWord(token, "RETURNING"))
            {
                break;
            }

            if (IsWord(token, "IN") && !IsSingleValueList(tokens, index + 1))
            {
                countable = false;
            }

            if (IsWord(token, "BETWEEN"))
            {
                between = true;
                terms++;
            }
            else if (IsWord(token, "LIKE") || IsWord(token, "GLOB"))
            {
                terms++;
            }
            else if (IsWord(token, "AND"))
            {
                if (between)
                {
                    between = false;
                }
                else
                {
                    terms++;
                }
            }
            else if (IsWord(token, "OR") || (IsWord(token, "NOT") && index + 1 < tokens.Count && !IsWord(tokens[index + 1], "NULL") &&
                                              !IsWord(tokens[index + 1], "BETWEEN") && !IsWord(tokens[index + 1], "IN") && !IsWord(tokens[index + 1], "LIKE")))
            {
                countable = false;
            }
        }

        end = index;
        return countable ? terms : -1;
    }

    /// <summary>Whether the <c>IN</c> operand at <paramref name="index"/> is a parenthesized list of exactly one value.</summary>
    private static bool IsSingleValueList(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (index >= tokens.Count || tokens[index].Kind != SqlTokenKind.OpenParenthesis ||
            (index + 1 < tokens.Count && (IsWord(tokens[index + 1], "SELECT") || IsWord(tokens[index + 1], "WITH") || IsWord(tokens[index + 1], "VALUES"))))
        {
            return false;
        }

        var depth = 0;
        for (var position = index; position < tokens.Count; position++)
        {
            if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if (tokens[position].Kind == SqlTokenKind.CloseParenthesis && --depth == 0)
            {
                return true;
            }
            else if (depth == 1 && tokens[position].Text == ",")
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>
    /// Whether a level reads one table and every <c>AND</c>-joined term of its <c>WHERE</c> lets each row the index finds
    /// count toward the limit. Each term must be bare comparisons of columns with values (<c>=</c>, <c>&lt;</c>,
    /// <c>&gt;</c>, <c>BETWEEN</c>, <c>IS NULL</c>, a one-value <c>IN</c>; no arithmetic, unary sign, <c>NOT</c>, call
    /// or subquery), and compare only columns the index search constrains, or be a keyset seek: compare only
    /// <c>ORDER BY</c> columns, the leading one among them, while the index reads a range of that leading column, so the
    /// seek rejects rows at its boundary only (<c>(Seen &lt; @s) OR (Seen = @s AND Id &gt; @id)</c>). With a partial sort,
    /// the index must read a range of the leading <c>ORDER BY</c> column too.
    /// </summary>
    private static bool ServesEveryCondition(IReadOnlyList<SqlToken> tokens, (int Start, int End) level, IReadOnlyList<(string Column, string Operator)> indexColumns, Func<string, bool>? partialSort)
    {
        var sources = 0;
        var depth = 0;
        var inFromList = false;
        var where = -1;
        var orderBy = -1;
        for (var position = level.Start; position < level.End; position++)
        {
            var token = tokens[position];
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                depth--;
                continue;
            }

            if (depth != 0)
            {
                continue;
            }

            if ((IsWord(token, "FROM") && !(position > level.Start && IsWord(tokens[position - 1], "DISTINCT"))) || IsWord(token, "JOIN") || IsWord(token, "UPDATE"))
            {
                sources++;
                inFromList = IsWord(token, "FROM");
            }
            else if (token.Text == "," && inFromList)
            {
                // FROM a, b
                sources++;
            }
            else if (token.Kind == SqlTokenKind.Word && NotAliases.Contains(token.Text) && !IsWord(token, "AS"))
            {
                inFromList = false;
                where = IsWord(token, "WHERE") ? position : where;
                orderBy = IsWord(token, "ORDER") && position + 1 < level.End && IsWord(tokens[position + 1], "BY") ? position + 2 : orderBy;
            }
        }

        if (sources != 1)
        {
            return false;
        }

        var indexed = new HashSet<string>(indexColumns.Select(constraint => constraint.Column), StringComparer.OrdinalIgnoreCase);
        var ordered = orderBy < 0 ? new List<string>() : OrderColumns(tokens, orderBy, level.End);
        var leadingRange = ordered.Count > 0 && indexColumns.Any(constraint =>
            string.Equals(constraint.Column, ordered[0], StringComparison.OrdinalIgnoreCase) && constraint.Operator is ">" or ">=" or "<" or "<=");
        if (partialSort != null && !(leadingRange && partialSort(ordered[0])))
        {
            return false;
        }

        if (where < 0)
        {
            return true;
        }

        foreach (var (columns, plain) in WhereTerms(tokens, where + 1, level.End))
        {
            var served = columns.All(indexed.Contains);
            var seek = leadingRange && columns.Contains(ordered[0], StringComparer.OrdinalIgnoreCase) && columns.All(column => ordered.Contains(column, StringComparer.OrdinalIgnoreCase));
            if (!plain || !(served || seek))
            {
                return false;
            }
        }

        return true;
    }

    // Words in a condition that are not columns.
    private static readonly HashSet<string> ConditionKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "AND", "OR", "NOT", "IN", "IS", "NULL", "BETWEEN", "LIKE", "GLOB", "ESCAPE", "CASE", "WHEN", "THEN", "ELSE", "END",
        "TRUE", "FALSE", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "DISTINCT", "FROM"
    };

    /// <summary>
    /// The <c>AND</c>-joined terms of a <c>WHERE</c> from <paramref name="index"/> to the end of its clause: the columns
    /// each compares (unqualified), and whether it is plain, bare comparisons of columns with values: no function call,
    /// subquery, arithmetic or unary sign, <c>NOT</c> (other than <c>IS NOT NULL</c>), <c>&lt;&gt;</c>, pattern or
    /// <c>IN</c> of several values. A <c>COLLATE</c> name is skipped: SQLite lists a constraint only under the index's
    /// collation.
    /// </summary>
    private static IEnumerable<(List<string> Columns, bool Plain)> WhereTerms(IReadOnlyList<SqlToken> tokens, int index, int end)
    {
        var columns = new List<string>();
        var plain = true;
        var depth = 0;
        var cases = 0;
        var between = false;
        for (; index < end; index++)
        {
            var token = tokens[index];
            var previous = index > 0 ? tokens[index - 1] : default;
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
                plain &= !(index + 1 < end && (IsWord(tokens[index + 1], "SELECT") || IsWord(tokens[index + 1], "WITH") || IsWord(tokens[index + 1], "VALUES")));
                continue;
            }

            if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                depth--;
                continue;
            }

            if (depth == 0 && (token.Kind == SqlTokenKind.Semicolon || IsWord(token, "LIMIT") || IsWord(token, "ORDER") || IsWord(token, "GROUP") ||
                               IsWord(token, "HAVING") || IsWord(token, "UNION") || IsWord(token, "EXCEPT") || IsWord(token, "INTERSECT") ||
                               IsWord(token, "WINDOW") || IsWord(token, "RETURNING")))
            {
                break;
            }

            if (IsWord(token, "CASE"))
            {
                cases++;
                plain = false;
            }
            else if (IsWord(token, "END") && cases > 0)
            {
                cases--;
            }
            else if (depth == 0 && IsWord(token, "BETWEEN"))
            {
                between = true;
            }
            else if (depth == 0 && cases == 0 && IsWord(token, "AND"))
            {
                if (between)
                {
                    between = false;
                    continue;
                }

                yield return (columns, plain);
                columns = new List<string>();
                plain = true;
                continue;
            }
            else if (IsWord(token, "COLLATE"))
            {
                index++;
                continue;
            }
            else if (IsWord(token, "IN"))
            {
                plain &= IsSingleValueList(tokens, index + 1);
                continue;
            }
            else if ((IsWord(token, "NOT") && !(index + 1 < end && IsWord(tokens[index + 1], "NULL"))) || IsWord(token, "LIKE") ||
                     IsWord(token, "GLOB") || IsWord(token, "REGEXP") || IsWord(token, "MATCH"))
            {
                plain = false;
                continue;
            }

            if (token.Kind == SqlTokenKind.Symbol)
            {
                // Comparisons and separators are fine, and so is arithmetic on values (Completed > @now - 3600,
                // BETWEEN -5 AND -1); an operator or sign next to a column (+Completed, Completed % 10, Name || 'x')
                // changes what the index compares. <> and != cannot seek.
                var comparison = token.Text is "<" or ">" or "=" or "," or ".";
                var notEqual = (token.Text == ">" && previous.Kind == SqlTokenKind.Symbol && previous.Text == "<" && previous.Position == token.Position - 1) ||
                               token.Text == "!";
                var touchesColumn = IsColumnOperand(tokens, index - 1, end) || IsColumnOperand(tokens, index + 1, end) ||
                                    (index > 0 && previous.Kind == SqlTokenKind.CloseParenthesis && GroupHasColumn(tokens, FindOpen(tokens, index - 1), index - 1)) ||
                                    (index + 1 < end && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis && GroupHasColumn(tokens, index + 1, FindClose(tokens, index + 1, end)));
                plain &= !notEqual && (comparison || !touchesColumn);
                continue;
            }

            if (token.Kind is not (SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier) || (token.Kind == SqlTokenKind.Word && ConditionKeywords.Contains(token.Text)))
            {
                continue;
            }

            if (index + 1 < end && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis)
            {
                // A call over values (unixepoch(), strftime('%s', 'now')) is a value; one over a column is not plain.
                var close = FindClose(tokens, index + 1, end);
                for (var inner = index + 2; inner < close; inner++)
                {
                    plain &= !IsColumnOperand(tokens, inner, close) && !IsWord(tokens[inner], "SELECT");
                }

                index = close;
                continue;
            }

            if (index + 2 < end && tokens[index + 1].Text == ".")
            {
                // A qualifier: the column is the last part.
                continue;
            }

            columns.Add(token.Value);
        }

        yield return (columns, plain);
    }

    /// <summary>Whether the token at <paramref name="index"/> names a column: an identifier that is not a keyword or a call.</summary>
    private static bool IsColumnOperand(IReadOnlyList<SqlToken> tokens, int index, int end)
        => index >= 0 && index < end &&
           (tokens[index].Kind == SqlTokenKind.QuotedIdentifier ||
            (tokens[index].Kind == SqlTokenKind.Word && !ConditionKeywords.Contains(tokens[index].Text) &&
             !(index + 1 < end && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis)));

    /// <summary>Whether a column appears between the parentheses at <paramref name="open"/> and <paramref name="close"/>.</summary>
    private static bool GroupHasColumn(IReadOnlyList<SqlToken> tokens, int open, int close)
    {
        for (var index = Math.Max(0, open + 1); index < close; index++)
        {
            if (IsColumnOperand(tokens, index, close))
            {
                return true;
            }
        }

        return open < 0;
    }

    /// <summary>The index of the parenthesis opening the one closed at <paramref name="close"/>, or -1.</summary>
    private static int FindOpen(IReadOnlyList<SqlToken> tokens, int close)
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

    /// <summary>The index of the parenthesis closing the one at <paramref name="open"/>, or <paramref name="end"/>.</summary>
    private static int FindClose(IReadOnlyList<SqlToken> tokens, int open, int end)
    {
        var depth = 0;
        for (var index = open; index < end; index++)
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

        return end;
    }

    /// <summary>
    /// The columns an <c>ORDER BY</c> starting at <paramref name="index"/> sorts by, in order, up to the first item that
    /// is not a plain column (an expression such as <c>+Seen</c> ends the list: what follows does not order the read).
    /// </summary>
    private static List<string> OrderColumns(IReadOnlyList<SqlToken> tokens, int index, int end)
    {
        var columns = new List<string>();
        var depth = 0;
        var itemStart = true;
        for (; index < end; index++)
        {
            var token = tokens[index];
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                depth--;
            }
            else if (depth == 0 && (IsWord(token, "LIMIT") || IsWord(token, "OFFSET") || token.Kind == SqlTokenKind.Semicolon))
            {
                break;
            }
            else if (depth == 0 && token.Text == ",")
            {
                itemStart = true;
                continue;
            }
            else if (depth == 0 && itemStart)
            {
                string? name = null;
                var after = index;
                if (token.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier)
                {
                    name = LastNamePart(tokens, index, out after);
                }

                if (name == null || !(after >= end || tokens[after].Text == "," || IsWord(tokens[after], "ASC") || IsWord(tokens[after], "DESC") ||
                                      IsWord(tokens[after], "NULLS") || IsWord(tokens[after], "COLLATE") || IsWord(tokens[after], "LIMIT") || IsWord(tokens[after], "OFFSET")))
                {
                    break;
                }

                columns.Add(name);
            }

            itemStart = false;
        }

        return columns;
    }

    /// <summary>Whether a query level stops after a few rows (see <see cref="HasLimitWhereRead"/>).</summary>
    private static bool StopsEarly(IReadOnlyList<SqlToken> tokens, (int Start, int End) level, double? maximumRows)
    {
        var depth = 0;
        var limited = level.Start >= 2 && tokens[level.Start - 1].Kind == SqlTokenKind.OpenParenthesis && IsWord(tokens[level.Start - 2], "EXISTS");
        for (var position = level.Start; position < level.End; position++)
        {
            var token = tokens[position];
            if (token.Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
                continue;
            }

            if (token.Kind == SqlTokenKind.CloseParenthesis)
            {
                depth--;
                continue;
            }

            if (depth != 0 || token.Kind != SqlTokenKind.Word)
            {
                continue;
            }

            var next = position + 1 < level.End ? tokens[position + 1] : default;
            if ((Aggregates.Contains(token.Text) && next.Kind == SqlTokenKind.OpenParenthesis) || (IsWord(token, "GROUP") && IsWord(next, "BY")))
            {
                return false;
            }

            if (IsWord(token, "LIMIT")) {
                limited = LimitReadsFewRows(tokens, position + 1, level.End, maximumRows);
                continue;
            }
            limited |=
                       (IsWord(token, "FETCH") && (IsWord(next, "FIRST") || IsWord(next, "NEXT"))) ||
                       (IsWord(token, "TOP") && position > 0 && (IsWord(tokens[position - 1], "SELECT") || IsWord(tokens[position - 1], "DISTINCT")) &&
                        next.Kind is SqlTokenKind.Number or SqlTokenKind.Parameter or SqlTokenKind.OpenParenthesis);
        }

        return limited;
    }

    /// <summary>Reads <c>[schema.]name</c> at <paramref name="index"/>, returning the last part and the position after it.</summary>
    private static string? LastNamePart(IReadOnlyList<SqlToken> tokens, int index, out int after)
    {
        after = index;
        if (index >= tokens.Count || !IsName(tokens[index]) || (tokens[index].Kind == SqlTokenKind.Word && NotAliases.Contains(tokens[index].Text)) ||
            (index + 1 < tokens.Count && tokens[index + 1].Kind == SqlTokenKind.OpenParenthesis))
        {
            return null;
        }

        var name = tokens[index].Value;
        while (index + 2 < tokens.Count && tokens[index + 1].Text == "." && IsName(tokens[index + 2]))
        {
            index += 2;
            name = tokens[index].Value;
        }

        after = index + 1;
        return name;
    }

    /// <summary>The alias at <paramref name="index"/> (<c>[AS] alias</c>), or null.</summary>
    private static string? AliasAt(IReadOnlyList<SqlToken> tokens, int index)
    {
        if (index < tokens.Count && IsWord(tokens[index], "AS"))
        {
            index++;
        }

        return index < tokens.Count && IsName(tokens[index]) && !(tokens[index].Kind == SqlTokenKind.Word && NotAliases.Contains(tokens[index].Text))
            ? tokens[index].Value
            : null;
    }

    /// <summary>The token range of the query level holding <paramref name="index"/>: inside its parentheses, or the statement.</summary>
    private static (int Start, int End) LevelAround(IReadOnlyList<SqlToken> tokens, int index)
    {
        var start = 0;
        var depth = 0;
        for (var position = index - 1; position >= 0; position--)
        {
            if (tokens[position].Kind == SqlTokenKind.CloseParenthesis)
            {
                depth++;
            }
            else if (tokens[position].Kind == SqlTokenKind.OpenParenthesis && depth-- == 0)
            {
                start = position + 1;
                break;
            }
            else if (tokens[position].Kind == SqlTokenKind.Semicolon && depth == 0)
            {
                start = position + 1;
                break;
            }
        }

        var end = tokens.Count;
        depth = 0;
        for (var position = index + 1; position < tokens.Count; position++)
        {
            if (tokens[position].Kind == SqlTokenKind.OpenParenthesis)
            {
                depth++;
            }
            else if ((tokens[position].Kind == SqlTokenKind.CloseParenthesis && depth-- == 0) ||
                     (tokens[position].Kind == SqlTokenKind.Semicolon && depth == 0))
            {
                end = position;
                break;
            }
        }

        return (start, end);
    }

    private static bool IsName(SqlToken token) => token.Kind is SqlTokenKind.Word or SqlTokenKind.QuotedIdentifier;

    private static bool IsWord(SqlToken token, string word)
        => token.Kind == SqlTokenKind.Word && string.Equals(token.Text, word, StringComparison.OrdinalIgnoreCase);
}
