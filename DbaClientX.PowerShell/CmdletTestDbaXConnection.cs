using System.Diagnostics;
using System.Data.Common;
using DBAClientX.Invoker;

namespace DBAClientX.PowerShell;

/// <summary>Validates and optionally pings a DbaClientX provider connection string.</summary>
/// <example>
/// <summary>Test a SQL Server connection string.</summary>
/// <prefix>PS&gt; </prefix>
/// <code>Test-DbaXConnection -Provider SqlServer -ConnectionString 'Server=.;Database=master;Integrated Security=True;Encrypt=True;TrustServerCertificate=True' -Detailed</code>
/// <para>Returns validation and ping details for the supplied SQL Server connection string.</para>
/// </example>
[Cmdlet(VerbsDiagnostic.Test, "DbaXConnection")]
[CmdletBinding()]
public sealed class CmdletTestDbaXConnection : PSCmdlet
{
    /// <summary>Database provider used to validate the connection string.</summary>
    [Parameter(Mandatory = true, Position = 0)]
    public DbaXProvider Provider { get; set; }

    /// <summary>Provider connection string, or a SQLite database path.</summary>
    [Parameter(Mandatory = true, Position = 1)]
    [ValidateNotNullOrEmpty]
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>Only validate connection-string shape and skip opening a provider connection.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter SkipPing { get; set; }

    /// <summary>Return a detailed result object instead of a Boolean.</summary>
    [Parameter(Mandatory = false)]
    public SwitchParameter Detailed { get; set; }

    /// <inheritdoc />
    protected override void ProcessRecord()
    {
        var alias = DbaXProviderHelpers.GetAlias(Provider);
        var validationConnectionString = Provider == DbaXProvider.SQLite
            ? DbaXProviderHelpers.GetSQLiteConnectionString(ConnectionString)
            : ConnectionString;
        var validation = DbaConnectionFactory.Validate(
            alias,
            validationConnectionString);

        var pingSucceeded = false;
        object? pingResult = null;
        string? pingError = null;
        var stopwatch = new Stopwatch();
        if (validation.IsValid && !SkipPing.IsPresent)
        {
            try
            {
                stopwatch.Start();
                pingResult = DbaXProviderHelpers.ExecutePing(Provider, ConnectionString);
                stopwatch.Stop();
                pingSucceeded = true;
            }
            catch (Exception ex) when (ex is DbaQueryExecutionException or DbException or InvalidOperationException or TimeoutException)
            {
                stopwatch.Stop();
                pingError = ex.Message;
            }
        }

        var succeeded = validation.IsValid && (SkipPing.IsPresent || pingSucceeded);
        if (!Detailed.IsPresent)
        {
            WriteObject(succeeded);
            return;
        }

        WriteObject(new PSObject(new
        {
            Provider,
            ConnectionStringValid = validation.IsValid,
            ValidationCode = validation.Code.ToString(),
            ValidationMessage = validation.IsValid ? null : DbaConnectionFactory.ToUserMessage(validation),
            PingAttempted = validation.IsValid && !SkipPing.IsPresent,
            PingSucceeded = pingSucceeded,
            PingResult = pingResult,
            PingError = pingError,
            ElapsedMilliseconds = Math.Round(stopwatch.Elapsed.TotalMilliseconds, 2),
            Succeeded = succeeded
        }));
    }
}
