using System;
using DBAClientX.Invoker;
using Xunit;

namespace DbaClientX.Tests;

public class DbaConnectionFactoryTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Validate_MissingProvider(string? provider)
    {
        var result = DbaConnectionFactory.Validate(provider ?? string.Empty, "Server=.;Database=app;");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingProvider, result.Code);
        Assert.False(result.IsValid);
    }

    [Fact]
    public void Validate_MissingConnectionString()
    {
        var result = DbaConnectionFactory.Validate("sqlserver", " ");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingConnectionString, result.Code);
        Assert.False(result.IsValid);
    }

    [Fact]
    public void Validate_UnsupportedProvider()
    {
        var result = DbaConnectionFactory.Validate("db2", "Server=.;Database=app;");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedProvider, result.Code);
        Assert.Equal("Provider 'db2' is not supported.", result.Message);
    }

    [Fact]
    public void Validate_MalformedConnectionString()
    {
        var result = DbaConnectionFactory.Validate("sqlserver", "Server==:bad");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MalformedConnectionString, result.Code);
        Assert.Contains("could not be parsed", DbaConnectionFactory.ToUserMessage(result));
    }

    [Fact]
    public void Validate_MissingParameters()
    {
        var result = DbaConnectionFactory.Validate("sqlserver", "Server=.;");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Contains("database", result.Message.ToLowerInvariant());
    }

    [Theory]
    [InlineData("Server=;Database=app;", "Server")]
    [InlineData("Server=dbhost;Database=;", "Database")]
    public void Validate_EmptyRequiredParameterIsMissing(string connectionString, string expectedParameter)
    {
        var result = DbaConnectionFactory.Validate("sqlserver", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal(expectedParameter, result.Details);
    }

    [Theory]
    [InlineData("AllowLoadLocalInfile")]
    [InlineData("Allow Load Local Infile")]
    [InlineData("LoadLocalInfile")]
    public void Validate_UnsupportedOption(string option)
    {
        var result = DbaConnectionFactory.Validate("mysql", $"Server=.;Database=app;{option}=true");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedOption, result.Code);
        Assert.Equal(option, result.Details, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("disabled", DbaConnectionFactory.ToUserMessage(result).ToLowerInvariant());
    }

    [Theory]
    [InlineData("AllowLoadLocalInfile")]
    [InlineData("Allow Load Local Infile")]
    [InlineData("LoadLocalInfile")]
    public void Validate_AllowedUnsupportedOption_StillRunsRemainingValidation(string option)
    {
        var result = DbaConnectionFactory.Validate(
            "mysql",
            $"Server=dbhost;Database=app;{option}=true",
            new[] { option });

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details);
    }

    [Theory]
    [InlineData("None")]
    [InlineData("Preferred")]
    [InlineData("Disabled")]
    [InlineData("Invalid")]
    public void Validate_MySqlConnectionString_WithNonEnforcingSslMode_IsRejected(string sslMode)
    {
        var connectionString = $"Server=dbhost;Database=app;User ID=user;Password=password;SslMode={sslMode}";
        var result = DbaConnectionFactory.Validate("mysql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedOption, result.Code);
        Assert.True(string.Equals("SSL Mode", result.Details, StringComparison.OrdinalIgnoreCase)
            || string.Equals("SslMode", result.Details, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_MySqlConnectionString_WithEmptySslMode_IsRejected()
    {
        var connectionString = "Server=dbhost;Database=app;User ID=user;Password=password;SslMode=";
        var result = DbaConnectionFactory.Validate("mysql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_MySqlConnectionString_WithWhitespaceSslMode_IsRejected()
    {
        var connectionString = "Server=dbhost;Database=app;User ID=user;Password=password;SslMode=   ";
        var result = DbaConnectionFactory.Validate("mysql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details, StringComparer.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("Required")]
    [InlineData("VerifyCA")]
    [InlineData("VerifyFull")]
    [InlineData("required")]
    public void Validate_MySqlConnectionString_WithEnforcingSslMode_Succeeds(string sslMode)
    {
        var connectionString = $"Server=dbhost;Database=app;User ID=user;Password=password;SslMode={sslMode}";
        var result = DbaConnectionFactory.Validate("mysql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_MySqlConnectionString_WithoutSslMode_IsRejected()
    {
        var connectionString = "Server=dbhost;Database=app;User ID=user;Password=password";
        var result = DbaConnectionFactory.Validate("mysql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details, StringComparer.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("False")]
    [InlineData("No")]
    [InlineData("Optional")]
    public void Validate_SqlServerConnectionString_WithDisabledEncryption_IsRejected(string encrypt)
    {
        var connectionString = $"Server=dbhost;Database=app;Encrypt={encrypt}";
        var result = DbaConnectionFactory.Validate("sqlserver", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedOption, result.Code);
        Assert.True(string.Equals("Encrypt", result.Details, StringComparison.OrdinalIgnoreCase)
            || string.Equals("Encryption", result.Details, StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData("Disable")]
    [InlineData("Allow")]
    [InlineData("Prefer")]
    [InlineData("Invalid")]
    public void Validate_PostgreSqlConnectionString_WithInsecureSslModes_IsRejected(string sslMode)
    {
        var connectionString = $"Server=dbhost;Database=app;Username=user;Password=password;SslMode={sslMode}";
        var result = DbaConnectionFactory.Validate("postgresql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedOption, result.Code);
        Assert.True(string.Equals("SSL Mode", result.Details, StringComparison.OrdinalIgnoreCase)
            || string.Equals("SslMode", result.Details, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Validate_PostgreSqlConnectionString_WithEmptySslMode_IsRejected()
    {
        var connectionString = "Server=dbhost;Database=app;Username=user;Password=password;SslMode=";
        var result = DbaConnectionFactory.Validate("postgresql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_PostgreSqlConnectionString_WithWhitespaceSslMode_IsRejected()
    {
        var connectionString = "Server=dbhost;Database=app;Username=user;Password=password;SslMode=   ";
        var result = DbaConnectionFactory.Validate("postgresql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details, StringComparer.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("Require")]
    [InlineData("VerifyCA")]
    [InlineData("VerifyFull")]
    [InlineData("require")]
    public void Validate_PostgreSqlConnectionString_WithEnforcingSslMode_Succeeds(string sslMode)
    {
        var connectionString = $"Server=dbhost;Database=app;Username=user;Password=password;SslMode={sslMode}";
        var result = DbaConnectionFactory.Validate("postgresql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_PostgreSqlConnectionString_WithoutSslMode_IsRejected()
    {
        var connectionString = "Server=dbhost;Database=app;Username=user;Password=password";
        var result = DbaConnectionFactory.Validate("postgresql", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("SslMode", result.Details, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_SqliteMissingFile()
    {
        var result = DbaConnectionFactory.Validate("sqlite", "Mode=ReadOnly");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Contains("data source", result.Message.ToLowerInvariant());
    }

    [Fact]
    public void Validate_SqliteFullUri_Succeeds()
    {
        var result = DbaConnectionFactory.Validate("sqlite", "FullUri=file:///C:/Temp/dbaclientx-test.db");

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_SqliteDottedFilenameIsAllowed()
    {
        var result = DbaConnectionFactory.Validate("sqlite", "Data Source=data..archive.db");

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_Success()
    {
        var result = DbaConnectionFactory.Validate("sqlserver", "Server=.;Database=app;");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
        Assert.Contains("validated", DbaConnectionFactory.ToUserMessage(result).ToLowerInvariant());
    }

    [Fact]
    public void Validate_InvalidPort()
    {
        var result = DbaConnectionFactory.Validate("postgresql", "Server=.;Database=app;Port=70000");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.InvalidParameterValue, result.Code);
        Assert.Equal("Port", result.Details, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("65535", result.Message);
    }

    [Fact]
    public void Validate_PrivilegedPort_IsAllowedWhenOtherwiseValid()
    {
        var result = DbaConnectionFactory.Validate("postgresql", "Server=.;Database=app;Port=22;SslMode=Require");

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Theory]
    [InlineData("sqlserver", "sqlserver", "DBAClientX.SqlServerGeneric.GenericExecutors")]
    [InlineData("mssql", "sqlserver", "DBAClientX.SqlServerGeneric.GenericExecutors")]
    [InlineData("postgres", "postgresql", "DBAClientX.PostgreSqlGeneric.GenericExecutors")]
    [InlineData("pgsql", "postgresql", "DBAClientX.PostgreSqlGeneric.GenericExecutors")]
    [InlineData("mysql", "mysql", "DBAClientX.MySqlGeneric.GenericExecutors")]
    [InlineData("sqlite", "sqlite", "DBAClientX.SQLiteGeneric.GenericExecutors")]
    [InlineData("oracle", "oracle", "DBAClientX.OracleGeneric.GenericExecutors")]
    public void TryGetProvider_UsesSharedAliasAndExecutorDescriptor(string alias, string canonicalName, string executorTypeName)
    {
        Assert.True(DbaConnectionFactory.TryGetProvider(alias, out var descriptor));
        Assert.Equal(canonicalName, descriptor.CanonicalName);
        Assert.Equal(executorTypeName, descriptor.GenericExecutorTypeName);
    }

    [Fact]
    public void Validate_OracleBuildConnectionString_Succeeds()
    {
        var connectionString = DBAClientX.Oracle.BuildConnectionString("dbhost", "svc", "user", "password");
        var result = DbaConnectionFactory.Validate("oracle", connectionString);

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_OracleExternalAuthenticationDoesNotRequirePassword()
    {
        var result = DbaConnectionFactory.Validate("oracle", "Data Source=dbhost/service;User Id=/");

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }

    [Fact]
    public void Validate_OraclePasswordAuthenticationRequiresPassword()
    {
        var result = DbaConnectionFactory.Validate("oracle", "Data Source=dbhost/service;User Id=user");

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Equal("Password", result.Details);
    }

    [Fact]
    public void Validate_ProviderAlias_IsTrimmed()
    {
        var result = DbaConnectionFactory.Validate("  sqlserver  ", "Server=.;Database=app;");

        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
    }
}
