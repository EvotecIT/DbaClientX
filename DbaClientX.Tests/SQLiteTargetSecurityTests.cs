using System.Collections.ObjectModel;
using System.Globalization;
using System.Management.Automation;
using System.Management.Automation.Host;
using System.Management.Automation.Runspaces;
using System.Security;
using System.Text;
using DBAClientX.PowerShell;

namespace DbaClientX.Tests;

public class SQLiteTargetSecurityTests
{
    private const string Secret = "example-target-secret";
    private const string Connection = "Data Source=:memory:;Password=" + Secret;

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void ConfirmationTargets_DoNotExposeConnectionOptions(bool transaction, bool confirm)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLite", typeof(CmdletInvokeDbaXSQLite), null));
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLiteTransaction", typeof(CmdletInvokeDbaXSQLiteTransaction), null));
        var host = new CaptureHost();
        using var runspace = RunspaceFactory.CreateRunspace(host, state);
        runspace.Open();
        using var ps = PowerShell.Create();
        ps.Runspace = runspace;
        ps.AddCommand(transaction ? "Invoke-DbaXSQLiteTransaction" : "Invoke-DbaXSQLite").AddParameter("Database", Connection);
        if (transaction) ps.AddParameter("ScriptBlock", ScriptBlock.Create("param($client)"));
        else ps.AddParameter("Query", "SELECT 1").AddParameter("ReadOnly");
        ps.AddParameter(confirm ? "Confirm" : "WhatIf", true);
        ps.Invoke();
        Assert.NotEmpty(host.Output.ToString());
        Assert.DoesNotContain(Secret, host.Output.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Password", host.Output.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void QueryFailure_TargetObjectDoesNotExposeConnectionOptions(bool stream, bool readOnly)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLite", typeof(CmdletInvokeDbaXSQLite), null));
        using var ps = PowerShell.Create(state);
        var connection = readOnly
            ? "Data Source=" + Path.Combine(Path.GetTempPath(), "dbaclientx-target-missing-" + Guid.NewGuid().ToString("N") + ".db") + ";Password=" + Secret
            : Connection;
        ps.AddCommand("Invoke-DbaXSQLite").AddParameter("Database", connection).AddParameter("Query", "SELECT 1")
            .AddParameter("ErrorAction", ActionPreference.Continue);
        if (stream) ps.AddParameter("Stream");
        if (readOnly) ps.AddParameter("ReadOnly");
        ps.Invoke();
        var error = Assert.Single(ps.Streams.Error);
        Assert.DoesNotContain(Secret, error.TargetObject?.ToString() ?? "", StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, error.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void MalformedMaintenanceInput_IsRejectedWithoutExposingConnectionOptions(bool destination)
    {
        var state = InitialSessionState.CreateDefault();
        state.Commands.Add(new SessionStateCmdletEntry("Invoke-DbaXSQLiteMaintenance", typeof(CmdletInvokeDbaXSQLiteMaintenance), null));
        var host = new CaptureHost();
        using var runspace = RunspaceFactory.CreateRunspace(host, state);
        runspace.Open();
        using var ps = PowerShell.Create();
        ps.Runspace = runspace;
        var malformed = "Data Source=app.db;Password='" + Secret;
        ps.AddCommand("Invoke-DbaXSQLiteMaintenance")
            .AddParameter("Database", destination ? "app.db" : malformed)
            .AddParameter("Action", destination ? DbaXSQLiteMaintenanceAction.Backup : DbaXSQLiteMaintenanceAction.Optimize)
            .AddParameter("WhatIf", true);
        if (destination) ps.AddParameter("Destination", malformed);
        var failure = Record.Exception(() => ps.Invoke());
        Assert.DoesNotContain(Secret, host.Output.ToString(), StringComparison.Ordinal);
        Assert.NotNull(failure);
        Assert.Contains("requires a valid SQLite connection string", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, failure.Message, StringComparison.Ordinal);
        if (failure is RuntimeException runtime)
            Assert.DoesNotContain(Secret, runtime.ErrorRecord.TargetObject?.ToString() ?? "", StringComparison.Ordinal);
    }

    private sealed class CaptureHost : PSHost
    {
        public StringBuilder Output { get; } = new();
        public override Guid InstanceId { get; } = Guid.NewGuid();
        public override string Name => "SQLiteTargetTest";
        public override Version Version => new(1, 0);
        public override CultureInfo CurrentCulture => CultureInfo.InvariantCulture;
        public override CultureInfo CurrentUICulture => CultureInfo.InvariantCulture;
        public override PSHostUserInterface UI => new CaptureUI(Output);
        public override void EnterNestedPrompt() { }
        public override void ExitNestedPrompt() { }
        public override void NotifyBeginApplication() { }
        public override void NotifyEndApplication() { }
        public override void SetShouldExit(int exitCode) { }
    }

    private sealed class CaptureUI(StringBuilder output) : PSHostUserInterface
    {
        public override PSHostRawUserInterface RawUI => null!;
        public override int PromptForChoice(string caption, string message, Collection<ChoiceDescription> choices, int defaultChoice)
        { output.AppendLine(caption).AppendLine(message); return choices.Select((choice, index) => new { choice, index }).First(item => item.choice.Label.Replace("&", "") == "No").index; }
        public override string ReadLine() => "";
        public override SecureString ReadLineAsSecureString() => new();
        public override void Write(string value) => output.Append(value);
        public override void Write(ConsoleColor foregroundColor, ConsoleColor backgroundColor, string value) => output.Append(value);
        public override void WriteLine(string value) => output.AppendLine(value);
        public override void WriteErrorLine(string value) => output.AppendLine(value);
        public override void WriteDebugLine(string value) => output.AppendLine(value);
        public override void WriteProgress(long sourceId, ProgressRecord record) { }
        public override void WriteVerboseLine(string value) => output.AppendLine(value);
        public override void WriteWarningLine(string value) => output.AppendLine(value);
        public override PSCredential PromptForCredential(string caption, string message, string userName, string targetName) => throw new NotSupportedException();
        public override PSCredential PromptForCredential(string caption, string message, string userName, string targetName, PSCredentialTypes types, PSCredentialUIOptions options) => throw new NotSupportedException();
        public override Dictionary<string, PSObject> Prompt(string caption, string message, Collection<FieldDescription> descriptions) => throw new NotSupportedException();
    }
}
