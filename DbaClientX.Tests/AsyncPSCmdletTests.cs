using System;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Management.Automation;
using System.Management.Automation.Host;
using System.Management.Automation.Runspaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Security;
using DBAClientX.PowerShell;
using Xunit;

public class AsyncPSCmdletTests
{
    [Cmdlet(VerbsDiagnostic.Test, "ShouldContinueCmdlet")]
    private class TestCmdlet : AsyncPSCmdlet
    {
        protected override Task ProcessRecordAsync()
        {
            bool result = ShouldContinue("Proceed?", "Question");
            WriteObject(result);
            return Task.CompletedTask;
        }
    }

    private class SimpleRawUI : PSHostRawUserInterface
    {
        public override ConsoleColor BackgroundColor { get; set; }
        public override Size BufferSize { get; set; }
        public override Coordinates CursorPosition { get; set; }
        public override int CursorSize { get; set; }
        public override ConsoleColor ForegroundColor { get; set; }
        public override bool KeyAvailable => false;
        public override Size MaxPhysicalWindowSize => new Size(120, 40);
        public override Size MaxWindowSize => new Size(120, 40);
        public override Coordinates WindowPosition { get; set; }
        public override Size WindowSize { get; set; }
        public override string WindowTitle { get; set; } = string.Empty;
        public override void FlushInputBuffer() { }
        public override BufferCell[,] GetBufferContents(Rectangle region) => new BufferCell[0,0];
        public override void SetBufferContents(Coordinates origin, BufferCell[,] contents) { }
        public override void SetBufferContents(Rectangle region, BufferCell fill) { }
        public override KeyInfo ReadKey(ReadKeyOptions options) => new();
        public override void ScrollBufferContents(Rectangle source, Coordinates destination, Rectangle clip, BufferCell fill) { }
    }

    private class TestHostUI : PSHostUserInterface
    {
        private readonly bool _answer;
        public TestHostUI(bool answer) => _answer = answer;
        public override PSHostRawUserInterface RawUI { get; } = new SimpleRawUI();
        public override int PromptForChoice(string caption, string message, Collection<ChoiceDescription> choices, int defaultChoice)
            => _answer ? 0 : 1;
        public override string ReadLine() => string.Empty;
        public override void Write(string message) { }
        public override void Write(ConsoleColor foregroundColor, ConsoleColor backgroundColor, string value) { }
        public override void WriteLine(string message) { }
        public override void WriteErrorLine(string message) { }
        public override void WriteDebugLine(string message) { }
        public override void WriteProgress(long sourceId, ProgressRecord record) { }
        public override void WriteVerboseLine(string message) { }
        public override void WriteWarningLine(string message) { }
        public override SecureString ReadLineAsSecureString() => new();
        public override PSCredential PromptForCredential(string caption, string message, string userName, string targetName)
            => new(userName, new SecureString());
        public override PSCredential PromptForCredential(string caption, string message, string userName, string targetName, PSCredentialTypes allowedCredentialTypes, PSCredentialUIOptions options)
            => PromptForCredential(caption, message, userName, targetName);
        public override Dictionary<string, PSObject> Prompt(string caption, string message, Collection<FieldDescription> descriptions)
            => new();
    }

    private class TestHost : PSHost
    {
        private readonly TestHostUI _ui;
        private readonly Guid _id = Guid.NewGuid();
        public TestHost(bool answer) => _ui = new TestHostUI(answer);
        public override Guid InstanceId => _id;
        public override string Name => "TestHost";
        public override Version Version => new Version(1, 0);
        public override PSHostUserInterface UI => _ui;
        public override CultureInfo CurrentCulture => CultureInfo.InvariantCulture;
        public override CultureInfo CurrentUICulture => CultureInfo.InvariantCulture;
        public override void EnterNestedPrompt() { }
        public override void ExitNestedPrompt() { }
        public override void NotifyBeginApplication() { }
        public override void NotifyEndApplication() { }
        public override void SetShouldExit(int exitCode) { }
    }

    private static object? RunCmdlet(bool answer)
    {
        var host = new TestHost(answer);
        // Use a minimal session state to avoid loading optional dependencies
        var iss = InitialSessionState.Create();
        iss.Commands.Add(new SessionStateCmdletEntry("Test-ShouldContinueCmdlet", typeof(TestCmdlet), null));
        using var runspace = RunspaceFactory.CreateRunspace(host, iss);
        runspace.Open();
        using var ps = PowerShell.Create();
        ps.Runspace = runspace;
        ps.AddCommand("Test-ShouldContinueCmdlet");
        var results = ps.Invoke();
        return results[0].BaseObject;
    }

    [Fact]
    public void ShouldContinue_ReturnsTrueWhenHostApproves()
    {
        var res = RunCmdlet(true);
        Assert.Equal(true, res);
    }

    [Fact]
    public void ShouldContinue_ReturnsFalseWhenHostDenies()
    {
        var res = RunCmdlet(false);
        Assert.Equal(false, res);
    }
}
