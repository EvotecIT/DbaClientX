using System;

namespace DBAClientX.QueryBuilder;

public partial class Query
{
    /// <summary>Adds a condition that a column contains <paramref name="text"/>, joined with <c>AND</c>.</summary>
    /// <param name="column">The column, quoted as an identifier.</param>
    /// <param name="text">The text to find. <c>%</c>, <c>_</c> and other pattern characters match themselves. It is a
    /// parameter with <c>CompileWithParameters</c>; <c>Compile</c> writes it as a quoted literal, which SQL Server
    /// converts to the database code page (prefer parameters for non-ASCII text).</param>
    /// <param name="folding">How letter case is treated; see <see cref="TextFolding"/>. MySQL's <c>LOWER</c> does not
    /// fold binary strings, and PostgreSQL needs a text expression.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <exception cref="ArgumentException">The column or the text is null, or the column is empty.</exception>
    /// <remarks>
    /// Compiles to <c>expr LIKE @p ESCAPE '!'</c> with <c>%</c>, <c>_</c>, <c>!</c> (and <c>[</c> on SQL Server) escaped
    /// in the pattern, so no character of <paramref name="text"/> acts as a wildcard. SQLite uses
    /// <c>instr(expr, @p) &gt; 0</c> instead, over <c>lower()</c> or <c>dbx_lower()</c> of the column when folding: its
    /// <c>LIKE</c> ignores ASCII case unless <c>PRAGMA case_sensitive_like</c> is on, stops at a NUL character and
    /// refuses patterns over 50,000 bytes, while <c>instr()</c> compares exactly and needs no escaping (it also ignores a
    /// <c>COLLATE NOCASE</c> column). Measured twice on 1,000,000 rows, <c>instr()</c> was as fast as or faster than an
    /// escaped <c>GLOB '*text*'</c> in every case (ASCII and other letters, rare and common text, with each folding;
    /// typically 5 to 15%); a plain <c>LIKE</c>, which folds ASCII itself, was 15 to 50% faster than
    /// <c>instr(lower())</c> but has the limits above.
    /// A NULL value does not contain anything, and neither matches <c>WhereNot</c> of the condition. An empty text
    /// matches every non-NULL value. A NUL character in the text compares as the database compares it: SQL Server's
    /// non-binary collations ignore it. A contains test cannot use an ordinary index, so it reads every row the other
    /// conditions leave.
    /// </remarks>
    public Query WhereContains(string column, string text, TextFolding folding)
        => AddContainsCondition(column, text, folding, logical: null, isRawExpression: false);

    /// <summary>Adds a condition that a column contains <paramref name="text"/>, joined with <c>AND</c>.</summary>
    /// <param name="column">The column, quoted as an identifier.</param>
    /// <param name="text">The text to find; see <see cref="WhereContains(string, string, TextFolding)"/>.</param>
    /// <param name="caseInsensitive"><see langword="true"/> for <see cref="TextFolding.Database"/> (on SQLite,
    /// ASCII letters only), <see langword="false"/> for <see cref="TextFolding.None"/>.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <remarks>See <see cref="WhereContains(string, string, TextFolding)"/>.</remarks>
    public Query WhereContains(string column, string text, bool caseInsensitive = false)
        => WhereContains(column, text, ToFolding(caseInsensitive));

    /// <summary>Adds a condition that a column contains <paramref name="text"/>, joined with <c>OR</c>.</summary>
    /// <inheritdoc cref="WhereContains(string, string, TextFolding)"/>
    public Query OrWhereContains(string column, string text, TextFolding folding)
        => AddContainsCondition(column, text, folding, "OR", isRawExpression: false);

    /// <summary>Adds a condition that a column contains <paramref name="text"/>, joined with <c>OR</c>.</summary>
    /// <inheritdoc cref="WhereContains(string, string, bool)"/>
    public Query OrWhereContains(string column, string text, bool caseInsensitive = false)
        => OrWhereContains(column, text, ToFolding(caseInsensitive));

    /// <summary>
    /// Adds a condition that a caller-authored SQL expression contains <paramref name="text"/>, joined with <c>AND</c>.
    /// </summary>
    /// <param name="expression">Trusted SQL emitted as written, without added parentheses: wrap an expression that
    /// contains operators. Never pass untrusted input.</param>
    /// <param name="text">The text to find; see <see cref="WhereContains(string, string, TextFolding)"/>.</param>
    /// <param name="folding">See <see cref="WhereContains(string, string, TextFolding)"/>.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    /// <remarks>Escaping, NULL and empty-text behavior are those of <see cref="WhereContains(string, string, TextFolding)"/>.</remarks>
    public Query WhereContainsRaw(string expression, string text, TextFolding folding)
        => AddContainsCondition(expression, text, folding, logical: null, isRawExpression: true);

    /// <summary>
    /// Adds a condition that a caller-authored SQL expression contains <paramref name="text"/>, joined with <c>AND</c>.
    /// </summary>
    /// <param name="expression">Trusted SQL emitted as written; see <see cref="WhereContainsRaw(string, string, TextFolding)"/>.</param>
    /// <param name="text">The text to find; see <see cref="WhereContains(string, string, TextFolding)"/>.</param>
    /// <param name="caseInsensitive">See <see cref="WhereContains(string, string, bool)"/>.</param>
    /// <returns>The current <see cref="Query"/> instance.</returns>
    public Query WhereContainsRaw(string expression, string text, bool caseInsensitive = false)
        => WhereContainsRaw(expression, text, ToFolding(caseInsensitive));

    /// <summary>
    /// Adds a condition that a caller-authored SQL expression contains <paramref name="text"/>, joined with <c>OR</c>.
    /// </summary>
    /// <inheritdoc cref="WhereContainsRaw(string, string, TextFolding)"/>
    public Query OrWhereContainsRaw(string expression, string text, TextFolding folding)
        => AddContainsCondition(expression, text, folding, "OR", isRawExpression: true);

    /// <summary>
    /// Adds a condition that a caller-authored SQL expression contains <paramref name="text"/>, joined with <c>OR</c>.
    /// </summary>
    /// <inheritdoc cref="WhereContainsRaw(string, string, bool)"/>
    public Query OrWhereContainsRaw(string expression, string text, bool caseInsensitive = false)
        => OrWhereContainsRaw(expression, text, ToFolding(caseInsensitive));

    private static TextFolding ToFolding(bool caseInsensitive) => caseInsensitive ? TextFolding.Database : TextFolding.None;

    private Query AddContainsCondition(string expression, string text, TextFolding folding, string? logical, bool isRawExpression)
    {
        ValidateString(expression, isRawExpression ? nameof(expression) : "column");
        if (text == null)
        {
            throw new ArgumentException("Text cannot be null.", nameof(text));
        }

        if (folding is not (TextFolding.None or TextFolding.Database or TextFolding.Invariant))
        {
            throw new ArgumentOutOfRangeException(nameof(folding), folding, "Unknown text folding.");
        }

        AddLogicalOperator(logical);
        _where.Add(new ContainsToken(expression, isRawExpression, text, folding));
        return this;
    }
}
