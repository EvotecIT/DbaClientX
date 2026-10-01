using System.Text;
using DBAClientX.QueryBuilder;

namespace DbaClientX.Tests;

/// <summary>
/// Literal SQL from <see cref="Query.Compile"/> must keep a string value inside its literal in every dialect,
/// including where a backslash is an escape character (MySQL by default, PostgreSQL with
/// <c>standard_conforming_strings</c> off).
/// </summary>
public sealed class QueryLiteralEscapingTests
{
    private const string Payload = @"x\' OR 1=1 -- ";

    [Fact]
    public void Compile_MySqlTextWithBackslash_UsesAHexLiteralThatReadsBackExactly()
    {
        var sql = new Query().Select("id").From("users").Where("name", Payload).Compile(SqlDialect.MySql);

        const string prefix = "SELECT `id` FROM `users` WHERE `name` = _utf8mb4 0x";
        Assert.StartsWith(prefix, sql);
        var hex = sql.Substring(prefix.Length);
        Assert.Matches("^[0-9A-F]+$", hex);
        Assert.Equal(Payload, Encoding.UTF8.GetString(Convert.FromHexString(hex)));
    }

    [Fact]
    public void Compile_PostgreSqlTextWithBackslash_UsesAnEscapeStringLiteral()
    {
        var sql = new Query().Select("id").From("users").Where("name", Payload).Compile(SqlDialect.PostgreSql);

        Assert.Equal("SELECT \"id\" FROM \"users\" WHERE \"name\" = E'x\\\\'' OR 1=1 -- '", sql);
    }

    [Theory]
    [InlineData(SqlDialect.SqlServer, "[name] = 'x\\'' OR 1=1 -- '")]
    [InlineData(SqlDialect.SQLite, "\"name\" = 'x\\'' OR 1=1 -- '")]
    [InlineData(SqlDialect.Oracle, "\"name\" = 'x\\'' OR 1=1 -- '")]
    public void Compile_DialectsWithLiteralBackslash_DoubleQuotesOnly(SqlDialect dialect, string expectedPredicate)
    {
        var sql = new Query().Select("id").From("users").Where("name", Payload).Compile(dialect);

        Assert.EndsWith(" WHERE " + expectedPredicate, sql);
    }

    private enum Status : short
    {
        Active = 7
    }

    [Fact]
    public void Compile_ValuesWithoutATextLiteralType_AreWrittenAsTypedLiterals()
    {
        var sql = new Query().Select("id").From("t")
            .Where("c", '\'')
            .Where("e", Status.Active)
            .Where("n", -5L)
            .Where("d", new DateOnly(2024, 1, 2))
            .Compile(SqlDialect.SqlServer);

        Assert.EndsWith("WHERE [c] = '''' AND [e] = 7 AND [n] = -5 AND [d] = '2024-01-02'", sql);
    }

    [Fact]
    public void Compile_ValueWithoutALiteralForm_IsRejectedInsteadOfEmittedRaw()
    {
        // ToString() of an unknown type would reach the SQL unquoted.
        var raw = new System.Text.StringBuilder("1 OR 1=1");

        Assert.Throws<NotSupportedException>(() => new Query().Select("id").From("t").Where("id", raw).Compile(SqlDialect.SqlServer));
        Assert.Throws<NotSupportedException>(() => new Query().Select("id").From("t").Where("x", double.NaN).Compile(SqlDialect.PostgreSql));
        var (_, parameters) = new Query().Select("id").From("t").Where("id", raw).CompileWithParameters(SqlDialect.SqlServer);
        Assert.Same(raw, parameters[0]);
    }

    [Theory]
    [InlineData(SqlDialect.MySql)]
    [InlineData(SqlDialect.PostgreSql)]
    public void Compile_TextWithoutBackslash_KeepsThePlainLiteral(SqlDialect dialect)
        => Assert.EndsWith(" = 'O''Brien'", new Query().Select("id").From("users").Where("name", "O'Brien").Compile(dialect));
}
