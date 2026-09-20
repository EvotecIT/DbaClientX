using DBAClientX;

namespace DbaClientX.Tests;

public sealed class DbaQueryExecutionExceptionTests
{
    [Fact]
    public void QueryText_IsNotExposedByMessageOrProperties()
    {
        const string query = "SELECT * FROM Users WHERE Password = 'super-secret'";

        var exception = new DbaQueryExecutionException("Failed to execute query.", query);

        Assert.DoesNotContain("super-secret", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(query, exception.ToString(), StringComparison.Ordinal);
        Assert.NotNull(exception.QueryFingerprint);
        Assert.Equal(64, exception.QueryFingerprint!.Length);
    }

    [Fact]
    public void EquivalentQueries_HaveTheSameFingerprint()
    {
        var first = new DbaQueryExecutionException("First", "SELECT 1");
        var second = new DbaQueryExecutionException("Second", "SELECT 1");

        Assert.Equal(first.QueryFingerprint, second.QueryFingerprint);
    }
}
