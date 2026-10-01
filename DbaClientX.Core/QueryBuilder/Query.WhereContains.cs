using System;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Adds a condition that a column contains <paramref name="text"/>, joined with <c>AND</c>.</summary>
    /// <param name="column">The column, quoted as an identifier.</param>
    /// <param name="text">The text to find. <c>%</c>, <c>_</c> and other pattern characters match themselves. It is a
    /// parameter with <c>CompileWithParameters</c>; <c>Compile</c> writes it as a quoted literal, which SQL Server
    /// converts to the database code page (prefer parameters for non-ASCII text).</param>
    /// <param name="caseInsensitive">
    /// <see langword="true"/> to fold both sides to lower case with the database's own function (<c>ILIKE</c> on
    /// PostgreSQL); SQLite's <c>lower()</c> folds ASCII letters only unless a Unicode function replaces it.
    /// <see langword="false"/> to match as the database compares text: <c>LIKE</c>, which follows the column's
    /// collation on SQL Server and MySQL (their default collations ignore case), and a case-sensitive <c>instr()</c>
    /// on SQLite, whose <c>LIKE</c> ignores ASCII case unless <c>PRAGMA case_sensitive_like</c> is on (<c>instr()</c>
    /// also ignores a <c>COLLATE NOCASE</c> column). MySQL's <c>LOWER</c> does not fold binary strings, and PostgreSQL
    /// needs a text expression.
    /// </param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <remarks>
    /// Compiles to <c>expr LIKE @p ESCAPE '!'</c> with <c>%</c>, <c>_</c>, <c>!</c> (and <c>[</c> on SQL Server) escaped
    /// in the pattern, so no character of <paramref name="text"/> acts as a wildcard. SQLite uses <c>instr()</c>
    /// instead (over <c>lower()</c> of both sides when folding), which needs no escaping and takes about 40% less
    /// time on a large scan. A NULL value does not contain
    /// anything, and neither matches <c>WhereNot</c> of the condition. An empty text matches every non-NULL value.
    /// A NUL character in the text compares as the database compares it: SQL Server's non-binary collations ignore it.
    /// A contains test cannot use an ordinary index, so it reads every row the other conditions leave.
    /// </remarks>
    public Query WhereContains(string column, string text, bool caseInsensitive = false)
        => AddContainsCondition(column, text, caseInsensitive, logical: null, isRawExpression: false);

    /// <summary>Adds a condition that a column contains <paramref name="text"/>, joined with <c>OR</c>.</summary>
    /// <inheritdoc cref="WhereContains(string, string, bool)"/>
    public Query OrWhereContains(string column, string text, bool caseInsensitive = false)
        => AddContainsCondition(column, text, caseInsensitive, "OR", isRawExpression: false);

    /// <summary>
    /// Adds a condition that a caller-authored SQL expression contains <paramref name="text"/>, joined with <c>AND</c>.
    /// </summary>
    /// <param name="expression">Trusted SQL emitted as written, without added parentheses: wrap an expression that
    /// contains operators. Never pass untrusted input.</param>
    /// <param name="text">The text to find; see <see cref="WhereContains(string, string, bool)"/>.</param>
    /// <param name="caseInsensitive">See <see cref="WhereContains(string, string, bool)"/>.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <remarks>Escaping, NULL and empty-text behavior are those of <see cref="WhereContains(string, string, bool)"/>.</remarks>
    public Query WhereContainsRaw(string expression, string text, bool caseInsensitive = false)
        => AddContainsCondition(expression, text, caseInsensitive, logical: null, isRawExpression: true);

    /// <summary>
    /// Adds a condition that a caller-authored SQL expression contains <paramref name="text"/>, joined with <c>OR</c>.
    /// </summary>
    /// <inheritdoc cref="WhereContainsRaw(string, string, bool)"/>
    public Query OrWhereContainsRaw(string expression, string text, bool caseInsensitive = false)
        => AddContainsCondition(expression, text, caseInsensitive, "OR", isRawExpression: true);

    private Query AddContainsCondition(string expression, string text, bool caseInsensitive, string? logical, bool isRawExpression)
    {
        ValidateString(expression, isRawExpression ? nameof(expression) : "column");
        if (text == null)
        {
            throw new ArgumentException("Text cannot be null.", nameof(text));
        }

        AddLogicalOperator(logical);
        _where.Add(new ContainsToken(expression, isRawExpression, text, caseInsensitive));
        return this;
    }
}
