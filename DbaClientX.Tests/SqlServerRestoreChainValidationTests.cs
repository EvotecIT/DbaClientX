using System.IO;
using DBAClientX;

namespace DbaClientX.Tests;

public sealed class SqlServerRestoreChainValidationTests
{
    private static readonly Guid Family = Guid.NewGuid();
    private static readonly Guid Fork = Guid.NewGuid();

    [Fact]
    public void LogOverlapAndLargeNativeLsnAreAcceptedWithoutRounding()
    {
        const decimal start = 1234567890123456789012345m;
        var full = Step(1, start, start + 100);
        var differential = Step(5, start + 200, start + 300, baseGuid: full.Header.BackupSetGuid);
        var log = Step(2, start + 250, start + 400);
        SqlServer.ValidateRestoreChain(new[] { full, differential, log });
        Assert.Equal(start + 400, log.Header.ChainMetadata!.LastLsn);
    }

    [Fact]
    public void CopyOnlyFullAndLaterConventionalBackupAllowContiguousLogs()
    {
        var full = Step(1, 100, 200, copyOnly: true);
        var log = Step(2, 150, 300, databaseBackupLsn: 225);
        SqlServer.ValidateRestoreChain(new[] { full, log });
    }

    [Theory]
    [InlineData(201, 300)] // Gap.
    [InlineData(100, 200)] // Does not advance.
    [InlineData(300, 200)] // Invalid range.
    public void DisconnectedOrNonAdvancingLogsAreRefused(decimal first, decimal last)
        => Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { Step(1, 100, 200), Step(2, first, last) }));

    [Fact]
    public void DifferentialMustUseTheExactConventionalFullBase()
    {
        var full = Step(1, 100, 200);
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { full, Step(5, 250, 300, baseGuid: Guid.NewGuid()) }));
        var copy = Step(1, 100, 200, copyOnly: true);
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { copy, Step(5, 250, 300, baseGuid: copy.Header.BackupSetGuid) }));
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { full, Step(2, 150, 250), Step(5, 250, 300, baseGuid: full.Header.BackupSetGuid) }));
    }

    [Theory]
    [InlineData("family")]
    [InlineData("fork")]
    [InlineData("transition")]
    [InlineData("damaged")]
    [InlineData("snapshot")]
    [InlineData("incomplete")]
    [InlineData("unchecked")]
    [InlineData("identity")]
    public void UnsafeOrUnpinnedHistoryIsRefused(string defect)
    {
        var full = Step(1, 100, 200);
        var log = Step(2, 150, 300, family: defect == "family" ? Guid.NewGuid() : Family,
            fork: defect == "fork" ? Guid.NewGuid() : Fork, transition: defect == "transition",
            damaged: defect == "damaged", snapshot: defect == "snapshot", incomplete: defect == "incomplete",
            checksums: defect != "unchecked", wrongIdentity: defect == "identity");
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { full, log }));
    }

    [Fact]
    public void MissingFullOrRepeatedBackupIsRefused()
    {
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(Array.Empty<SqlServerRestoreChainStep>()));
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { Step(2, 100, 200) }));
        var full = Step(1, 100, 200);
        Assert.Throws<InvalidDataException>(() => SqlServer.ValidateRestoreChain(new[] { full, full }));
    }

    [Fact]
    public async Task UnsupportedBackupPolicyAndEmptyChainFailBeforeConnecting()
    {
        using var provider = new SqlServer();
        const string unavailable = "Server=unavailable;Database=master;Integrated Security=True;Encrypt=True;Connect Timeout=1";
        await Assert.ThrowsAsync<ArgumentException>(() => provider.BackupToDedicatedDiskAsync(unavailable, "App", "C:\\Backup",
            SqlServerDiskBackupKind.Differential, copyOnly: true));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => provider.BackupToDedicatedDiskAsync(unavailable, "App", "C:\\Backup",
            (SqlServerDiskBackupKind)99, copyOnly: false));
        await Assert.ThrowsAsync<ArgumentException>(() => provider.PrepareRestoreChainAsNewAsync(unavailable,
            Array.Empty<SqlServerRestoreChainSource>(), "Target", new Dictionary<string, string> { ["App"] = "C:\\Data\\app.mdf" }));
    }

    private static SqlServerRestoreChainStep Step(int kind, decimal first, decimal last, bool copyOnly = false,
        Guid? baseGuid = null, decimal? databaseBackupLsn = null, Guid? family = null, Guid? fork = null,
        bool transition = false, bool damaged = false, bool snapshot = false, bool incomplete = false,
        bool checksums = true, bool wrongIdentity = false)
    {
        var sequence = new SqlServerBackupChainMetadata(first, last, first, databaseBackupLsn ?? 100,
            kind == 5 ? 100 : null, baseGuid, transition ? Guid.NewGuid() : fork ?? Fork, fork ?? Fork,
            transition ? first : null, snapshot, incomplete);
        var header = new SqlServerDiskBackupHeader("App", Guid.NewGuid(), family ?? Family, "Media", Guid.NewGuid(),
            kind, copyOnly, checksums, damaged, sequence);
        var expected = wrongIdentity ? new SqlServerBackupIdentity("App", Guid.NewGuid(), "Media", header.MediaSetId) : header.Identity;
        return new SqlServerRestoreChainStep(new SqlServerRestoreChainSource("C:\\Backup\\" + header.BackupSetGuid + ".bak", expected), header);
    }
}
