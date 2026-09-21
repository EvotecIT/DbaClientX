using DBAClientX;
using DBAClientX.DataMovement;
using Npgsql;

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
        Assert.Null(exception.ProviderSqlState);
        Assert.Equal(DbaProviderErrorKind.Unknown, exception.ProviderErrorKind);
        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT secret", exception.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("query-secret", exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ProviderClassification_IsRetainedWithoutProviderExceptionDetails()
    {
        const string secret = "native-provider-secret";

        var exception = new DbaQueryExecutionException(
            "Failed to execute query.",
            "SELECT * FROM MissingRows",
            new InvalidOperationException(secret),
            providerErrorCode: 208,
            providerSqlState: "42P01",
            providerErrorKind: DbaProviderErrorKind.MissingTable);

        Assert.Equal(208, exception.ProviderErrorCode);
        Assert.Equal("42P01", exception.ProviderSqlState);
        Assert.Equal(DbaProviderErrorKind.MissingTable, exception.ProviderErrorKind);
        Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void LegacyConstructor_PreservesNestedSanitizedProviderMetadata()
    {
        var original = new DbaQueryExecutionException(
            "First failure.",
            "SELECT first",
            new InvalidOperationException("provider-secret"),
            providerErrorCode: 1146,
            providerSqlState: "42S02",
            providerErrorKind: DbaProviderErrorKind.MissingTable);

        var wrapped = new DbaQueryExecutionException("Second failure.", "SELECT second", original);

        Assert.Equal(1146, wrapped.ProviderErrorCode);
        Assert.Equal("42S02", wrapped.ProviderSqlState);
        Assert.Equal(DbaProviderErrorKind.MissingTable, wrapped.ProviderErrorKind);
        Assert.Equal(typeof(InvalidOperationException).FullName, wrapped.ProviderExceptionType);
    }

    [Fact]
    public void ProviderFactory_ExtractsPostgreSqlStateAndClassification()
    {
        using var provider = new PostgreSqlExceptionFactory();
        var native = new PostgresException(
            "provider-secret",
            "ERROR",
            "ERROR",
            PostgresErrorCodes.UndefinedTable);

        DbaQueryExecutionException exception = provider.Wrap(native);

        Assert.Equal(PostgresErrorCodes.UndefinedTable, exception.ProviderSqlState);
        Assert.Equal(DbaProviderErrorKind.MissingTable, exception.ProviderErrorKind);
        Assert.Equal(typeof(PostgresException).FullName, exception.ProviderExceptionType);
        Assert.DoesNotContain("provider-secret", exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void SqliteMissingTable_RetainsPortableClassificationEndToEnd()
    {
        string path = Path.Combine(Path.GetTempPath(), $"dbaclientx-{Guid.NewGuid():N}.db");
        try
        {
            using var sqlite = new SQLite();
            var exception = Assert.Throws<DbaQueryExecutionException>(() =>
                sqlite.Query(path, "SELECT * FROM MissingRows"));

            Assert.Equal(DbaProviderErrorKind.MissingTable, exception.ProviderErrorKind);
            Assert.Equal(1, exception.ProviderErrorCode);
            var adapter = new SQLiteTableCopyAdapter($"Data Source={path}");
            Assert.True(((IDbaTableCopyMissingTableClassifier)adapter).IsMissingTableException(exception));
        }
        finally
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    private sealed class PostgreSqlExceptionFactory : PostgreSql
    {
        internal DbaQueryExecutionException Wrap(Exception exception)
            => CreateQueryExecutionException("Failed to execute stored procedure.", "secret_procedure", exception);
    }

}
