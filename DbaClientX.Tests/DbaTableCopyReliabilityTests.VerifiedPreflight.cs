using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopyReliabilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_BatchVerifiedPreflightPreservesMissingOptionalSourceContract(bool checkpoint)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath,
            "CREATE TABLE OtherRows (GroupName TEXT NOT NULL, Number INTEGER NOT NULL, Payload TEXT NULL, PRIMARY KEY(GroupName,Number)); " +
            "INSERT INTO OtherRows VALUES ('keep',1,'clear-as-empty')");
        var source = new SQLiteTableCopyAdapter(fixture.SourcePath, treatMissingTablesAsEmpty: true);
        var optional = fixture.Definition with { SourceName = "MissingSource", DestinationName = "OtherRows" };
        var result = await new DbaTableCopyEngine().CopyAsync(source, fixture.Destination,
            new[] { fixture.Definition, optional }, new()
            {
                ClearDestination = true, VerifyContent = !checkpoint,
                CheckpointId = checkpoint ? "optional-source" : null,
                PageSize = 3
            });

        Assert.True(result.Verified);
        Assert.Equal(12, result.CopiedRows);
        Assert.Equal(12, fixture.DestinationCount());
        Assert.Equal(0L, Convert.ToInt64(sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT COUNT(*) FROM OtherRows")));
        Assert.Equal(0, result.Tables[1].SourceRows);
        Assert.Equal(0, result.Tables[1].CopiedRows);
        if (checkpoint)
        {
            var saved = await fixture.Destination.ReadCheckpointAsync(optional);
            Assert.NotNull(saved);
            Assert.True(saved.Completed);
            Assert.Equal(0, saved.CopiedRows);
        }
    }

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
