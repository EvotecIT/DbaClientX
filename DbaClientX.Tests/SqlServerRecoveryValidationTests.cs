using DBAClientX;

namespace DbaClientX.Tests;

public sealed class SqlServerRecoveryValidationTests
{
    [Theory]
    [InlineData("relative.mdf")]
    [InlineData("C:relative.mdf")]
    [InlineData("C:\\data\\..\\existing.mdf")]
    [InlineData("/data/../existing.mdf")]
    [InlineData("/data/./target.mdf")]
    public async Task Preflight_RejectsAmbiguousPathsBeforeConnecting(string destination)
    {
        using var provider = new SqlServer();
        var identity = new SqlServerBackupIdentity("App", Guid.NewGuid(), "Media", Guid.NewGuid());
        await Assert.ThrowsAsync<ArgumentException>(() => provider.PrepareRestoreAsNewAsync(
            "Server=unavailable;Integrated Security=True;Connect Timeout=1", "/backups/app.bak", "App_Restore",
            identity, new Dictionary<string, string> { ["App"] = destination }));
    }

    [Fact]
    public async Task Preflight_RejectsEquivalentWindowsDestinationsBeforeConnecting()
    {
        using var provider = new SqlServer();
        var identity = new SqlServerBackupIdentity("App", Guid.NewGuid(), "Media", Guid.NewGuid());
        await Assert.ThrowsAsync<ArgumentException>(() => provider.PrepareRestoreAsNewAsync(
            "Server=unavailable;Integrated Security=True;Connect Timeout=1", "C:\\backups\\app.bak", "App_Restore",
            identity, new Dictionary<string, string>
            {
                ["App"] = "C:\\Data\\target.mdf",
                ["App_log"] = "c:/data/target.mdf"
            }));
    }
}
