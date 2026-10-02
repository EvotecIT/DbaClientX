using DBAClientX.QueryBuilder;
using Xunit;

namespace DbaClientX.Tests;

public sealed class QueryBuilderFunctionTests
{
    [Fact]
    public void WhereFunctionRaw_BindsArgumentsAndRefreshesValuesOnCacheHit()
    {
        foreach (string value in new[] { "ann'; SELECT 2; --", "bob" })
        {
            object[] arguments = { value, ";" };
            Query query = new Query().From("Rows").Where("Active", 1).WhereFunctionRaw("has_token", "\"Keys\"", "=", 1, arguments);
            arguments[0] = "changed";
            var compiled = query.CompileWithParameters(SqlDialect.SQLite);
            Assert.Contains("has_token(\"Keys\", @p1, @p2) = @p3", compiled.Sql);
            Assert.Equal(new object[] { 1, value, ";", 1 }, compiled.Parameters);
            Assert.DoesNotContain(value, compiled.Sql);
        }
    }

    [Theory]
    [InlineData("f(); DELETE FROM Rows", "=")]
    [InlineData("f", "OR 1=1")]
    [InlineData("f", "IN")]
    public void WhereFunctionRaw_InvalidFunctionOrOperator_RejectsWithoutChangingQuery(string function, string op)
    {
        Query query = new Query().From("Rows");
        Assert.Throws<System.ArgumentException>(() => query.WhereFunctionRaw(function, "\"Keys\"", op, 1, "ann", ";"));
        Assert.DoesNotContain("WHERE", query.Compile(SqlDialect.SQLite));
    }
}
