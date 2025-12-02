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
}
