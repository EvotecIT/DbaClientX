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

    [Fact]
    public void Validate_UnsupportedOption()
    {
        var result = DbaConnectionFactory.Validate("sqlserver", "Server=.;Database=app;UnsupportedOption=true");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedOption, result.Code);
        Assert.Equal("UnsupportedOption", result.Details, ignoreCase: true);
        Assert.Contains("unsupported", DbaConnectionFactory.ToUserMessage(result).ToLowerInvariant());
    }

    [Fact]
    public void Validate_SqliteMissingFile()
    {
        var result = DbaConnectionFactory.Validate("sqlite", "Mode=ReadOnly");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingRequiredParameter, result.Code);
        Assert.Contains("data source", result.Message.ToLowerInvariant());
    }

    [Fact]
    public void Validate_Success()
    {
        var result = DbaConnectionFactory.Validate("sqlserver", "Server=.;Database=app;");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.None, result.Code);
        Assert.True(result.IsValid);
        Assert.Contains("validated", DbaConnectionFactory.ToUserMessage(result).ToLowerInvariant());
    }
}
