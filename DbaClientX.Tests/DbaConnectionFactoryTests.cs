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

    [Fact]
    public void Validate_UnsupportedOption()
    {
        var result = DbaConnectionFactory.Validate("mysql", "Server=.;Database=app;AllowLoadLocalInfile=true");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.UnsupportedOption, result.Code);
        Assert.Equal("AllowLoadLocalInfile", result.Details, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("disabled", DbaConnectionFactory.ToUserMessage(result).ToLowerInvariant());
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

    [Fact]
    public void Validate_InvalidPort()
    {
        var result = DbaConnectionFactory.Validate("postgresql", "Server=.;Database=app;Port=70000");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.InvalidParameterValue, result.Code);
        Assert.Equal("Port", result.Details, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("65535", result.Message);
    }

    [Fact]
    public void Validate_ReservedPort()
    {
        var result = DbaConnectionFactory.Validate("postgresql", "Server=.;Database=app;Port=22");
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.InvalidParameterValue, result.Code);
        Assert.Equal("Port", result.Details, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("reserved system range", result.Message, StringComparison.OrdinalIgnoreCase);
    }
}
