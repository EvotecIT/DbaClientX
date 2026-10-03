using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopyReliabilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_BatchVerifiedPreflightFailurePreservesEveryDestinationAndReleasesItsTransaction(bool cancel)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath,
            "CREATE TABLE OtherRows (GroupName TEXT NOT NULL, Number INTEGER NOT NULL, Payload TEXT UNIQUE, PRIMARY KEY(GroupName,Number)); " +
            "INSERT INTO DestinationRows VALUES ('keep',1,'original'); INSERT INTO OtherRows VALUES ('keep',1,'other-original')");
        using var cancellation = new CancellationTokenSource();
        var definitions = new[] { fixture.Definition, fixture.Definition with { DestinationName = "OtherRows" } };
        var options = new DbaTableCopyOptions
        {
            ClearDestination = true, VerifyContent = true, CheckpointId = "batch-preserve", PageSize = 1,
            Progress = cancel ? snapshot =>
            {
                if (snapshot.Phase == DbaTableCopyPhase.ValidateSource) cancellation.Cancel();
            } : null
        };
        if (cancel)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination, definitions, options, cancellation.Token));
        else
        {
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination, definitions, options));
            Assert.Contains("schema preflight", error.Message, StringComparison.OrdinalIgnoreCase);
        }
        Assert.Equal(1, fixture.DestinationCount());
        Assert.Equal("original", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM DestinationRows"));
        Assert.Equal("other-original", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM OtherRows"));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(definitions[1]));
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "INSERT INTO OtherRows VALUES ('after',2,'unlocked')");
        Assert.Equal(2L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT COUNT(*) FROM OtherRows")));
    }
}
