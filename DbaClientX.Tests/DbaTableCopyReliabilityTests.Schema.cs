using System.Data;
using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopyReliabilityTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_SqliteEmbeddedNull_RoundTripsCompleteText(bool checkpoint)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        byte[] textBytes = System.Text.Encoding.UTF8.GetBytes("before\0Zażółć 😀 日本語");
        sqlite.ExecuteNonQuery(fixture.SourcePath, "UPDATE SourceRows SET Payload=CAST(@bytes AS TEXT) WHERE GroupName='a' AND Number=1", new Dictionary<string, object?> { ["@bytes"] = textBytes });
        DbaTableCopyResult result = await fixture.CopyAsync(new() { VerifyContent = true, CheckpointId = checkpoint ? "null-text" : null, PageSize = 12, BatchSize = 2 });
        Assert.True(result.Verified);
        Assert.Equal(Convert.ToHexString(textBytes), sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT hex(Payload) FROM DestinationRows WHERE GroupName='a' AND Number=1"));
        Assert.Equal("text", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT typeof(Payload) FROM DestinationRows WHERE GroupName='a' AND Number=1"));
    }

    [Fact]
    public async Task CopyAsync_MalformedSqliteText_PreservesDestinationBeforeClear()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.SourcePath, "UPDATE SourceRows SET Payload=CAST(x'80' AS TEXT)");
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "INSERT INTO DestinationRows VALUES ('keep',1,'original')");
        await Assert.ThrowsAsync<System.Text.DecoderFallbackException>(() => fixture.CopyAsync(new() { VerifyContent = true, ClearDestination = true, MaxPageBytes = 4096 }));
        Assert.Equal("original", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT Payload FROM DestinationRows"));
    }

    [Fact]
    public async Task ReadPageAsync_SqliteUnicodeAndEmbeddedNull_PreservesExactText()
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        string text = new string('x', 4095) + "😀\0Zażółć 日本語";
        sqlite.ExecuteNonQuery(fixture.SourcePath, "UPDATE SourceRows SET Payload=CAST(@bytes AS TEXT)", new Dictionary<string, object?> { ["@bytes"] = System.Text.Encoding.UTF8.GetBytes(text) });
        using DbaTableCopyPage page = await fixture.Source.ReadPageAsync(new(fixture.Definition, null, 1) { MaxBytes = text.Length * 2L + 1024 });
        Assert.Equal(text, page.Data.Rows[0]["Payload"]);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_UnwritableSqliteDestinationSchema_PreservesExistingRows(bool generated)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        string extra = generated ? "Payload TEXT GENERATED ALWAYS AS ('generated') STORED" : "Payload TEXT, RequiredValue INTEGER NOT NULL";
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "DROP TABLE DestinationRows; CREATE TABLE DestinationRows (GroupName TEXT NOT NULL, Number INTEGER NOT NULL, " + extra + ", PRIMARY KEY(GroupName,Number))");
        sqlite.ExecuteNonQuery(fixture.DestinationPath, generated
            ? "INSERT INTO DestinationRows (GroupName,Number) VALUES ('keep',1)"
            : "INSERT INTO DestinationRows VALUES ('keep',1,'original',7)");
        await Assert.ThrowsAsync<InvalidOperationException>(() => fixture.CopyAsync(new() { ClearDestination = true, CheckpointId = "schema" }));
        Assert.Equal("keep", sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT GroupName FROM DestinationRows"));
        Assert.Null(await fixture.Destination.ReadCheckpointAsync(fixture.Definition));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CopyAsync_UnmappedSqliteDefaultOrGeneratedColumn_PreservesSupportedWrites(bool generated)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.DestinationPath, "ALTER TABLE DestinationRows ADD Extra " + (generated ? "INTEGER GENERATED ALWAYS AS (7) VIRTUAL" : "INTEGER NOT NULL DEFAULT 7"));
        DbaTableCopyResult result = await fixture.CopyAsync(new() { CheckpointId = "defaults" });
        Assert.True(result.Verified);
        Assert.Equal(12L, sqlite.ExecuteScalar(fixture.DestinationPath, "SELECT COUNT(*) FROM DestinationRows WHERE Extra=7"));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ReadPageAsync_MixedSqliteKeysAndPayloads_PreservesRuntimeTypes(bool keyset)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        sqlite.ExecuteNonQuery(fixture.SourcePath, "CREATE TABLE MixedKeys (Id NUMERIC PRIMARY KEY, Payload); INSERT INTO MixedKeys VALUES (1,1),(1.5,x'0080FF'),('word','text')");
        var definition = new DbaTableCopyDefinition("MixedKeys", "MixedKeys", new[] { "Id" }) { UseKeysetPagination = keyset };
        using DbaTableCopyPage first = await fixture.Source.ReadPageAsync(new(definition, null, 2) { MaxBytes = keyset ? 4096 : null });
        Assert.Equal(1.5d, first.Data.Rows[1]["Id"]);
        Assert.Equal(new byte[] { 0, 128, 255 }, Assert.IsType<byte[]>(first.Data.Rows[1]["Payload"]));
        using DbaTableCopyPage next = await fixture.Source.ReadPageAsync(new(definition, first.ContinuationToken, 2) { MaxBytes = keyset ? 4096 : null });
        Assert.Equal("word", Assert.Single(next.Data.Rows.Cast<DataRow>())["Id"]);
    }
}
