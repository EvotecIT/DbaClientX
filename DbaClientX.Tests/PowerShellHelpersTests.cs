using System;
using System.Collections.Generic;
using System.Management.Automation;
using DBAClientX.Invoker;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class PowerShellHelpersTests
{
    private sealed class TestTerminatingErrorException : Exception
    {
        public TestTerminatingErrorException(ErrorRecord errorRecord) : base(errorRecord.ToString())
        {
            ErrorRecord = errorRecord;
        }

        public ErrorRecord ErrorRecord { get; }
    }

    private sealed class FakeCmdlet : PSCmdlet
    {
    }

    [Fact]
    public void TryValidateConnection_InvalidConnection_WritesWarning()
    {
        var cmdlet = new FakeCmdlet();

        var warnings = new List<string>();

        var success = PowerShellHelpers.TryValidateConnection(
            cmdlet,
            "sqlserver",
            string.Empty,
            ActionPreference.Continue,
            warnings.Add);

        Assert.False(success);
        Assert.Single(warnings);
        Assert.Contains("connection string", warnings[0], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TryValidateConnection_StopPreference_ThrowsTerminatingError()
    {
        var cmdlet = new FakeCmdlet();
        ErrorRecord? terminatingError = null;

        var exception = Assert.Throws<TestTerminatingErrorException>(() =>
            PowerShellHelpers.TryValidateConnection(
                cmdlet,
                "sqlserver",
                string.Empty,
                ActionPreference.Stop,
                _ => { },
                error =>
                {
                    terminatingError = error;
                    throw new TestTerminatingErrorException(error);
                }));

        Assert.NotNull(exception.ErrorRecord);
        Assert.Equal(DbaConnectionFactory.ConnectionValidationErrorCode.MissingConnectionString.ToString(), exception.ErrorRecord.FullyQualifiedErrorId);
        Assert.Equal(ErrorCategory.InvalidArgument, exception.ErrorRecord.CategoryInfo.Category);
        Assert.Same(terminatingError, exception.ErrorRecord);
    }

    [Fact]
    public void ResolveSqlServerCredential_UsesIntegratedSecurity_WhenNoCredentialsProvided()
    {
        var result = PowerShellHelpers.ResolveSqlServerCredential(string.Empty, string.Empty, null);

        Assert.True(result.IntegratedSecurity);
        Assert.Equal(string.Empty, result.Username);
        Assert.Equal(string.Empty, result.Password);
    }

    [Fact]
    public void ResolveSqlServerCredential_PrefersPSCredential_WhenProvided()
    {
        using var securePassword = new System.Security.SecureString();
        foreach (var character in "secret")
        {
            securePassword.AppendChar(character);
        }

        var credential = new PSCredential("sql-user", securePassword);

        var result = PowerShellHelpers.ResolveSqlServerCredential("ignored", "ignored", credential);

        Assert.False(result.IntegratedSecurity);
        Assert.Equal("sql-user", result.Username);
        Assert.Equal("secret", result.Password);
    }

    [Fact]
    public void ResolveExplicitCredential_PrefersPSCredential_WhenProvided()
    {
        using var securePassword = new System.Security.SecureString();
        foreach (var character in "secret")
        {
            securePassword.AppendChar(character);
        }

        var credential = new PSCredential("provider-user", securePassword);

        var result = PowerShellHelpers.ResolveExplicitCredential("ignored", "ignored", credential, "MySQL");

        Assert.Equal("provider-user", result.Username);
        Assert.Equal("secret", result.Password);
    }

    [Fact]
    public void ResolveExplicitCredential_Throws_WhenCredentialsAreMissing()
    {
        var exception = Assert.Throws<PSArgumentException>(() =>
            PowerShellHelpers.ResolveExplicitCredential(string.Empty, string.Empty, null, "Oracle"));

        Assert.Contains("-Credential", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Oracle", exception.Message, StringComparison.Ordinal);
    }
}
