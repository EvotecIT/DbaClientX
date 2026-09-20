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

    [Fact]
    public void ProviderException_IsReducedToNonSensitiveMetadata()
    {
        const string secret = "provider-secret-value";
        var providerException = new InvalidOperationException(secret);
        providerException.Data["command"] = "SELECT secret FROM credentials";

        var exception = new DbaQueryExecutionException(
            "Failed to execute query.",
            "SELECT * FROM credentials WHERE password = 'query-secret'",
            providerException);

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        Assert.NotSame(providerException, exception.InnerException);
        Assert.Equal(typeof(InvalidOperationException).FullName, exception.ProviderExceptionType);
        Assert.Null(exception.ProviderErrorCode);
        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT secret", exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("query-secret", exception.ToString(), StringComparison.Ordinal);
    }
}
