using DBAClientX;
using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed partial class DbaTableCopyReliabilityTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public async Task CopyAsync_ConversionProjection_VerifiesAndResumesEffectiveValues(int mode)
    {
        using var fixture = new Fixture();
        using var sqlite = new SQLite();
        string destinationColumn = mode is 1 or 2 ? "CopiedText" : mode == 4 ? "text" : "Text";
        sqlite.ExecuteNonQuery(fixture.SourcePath, "CREATE TABLE ConversionRows (Id INTEGER PRIMARY KEY, Text TEXT); INSERT INTO ConversionRows VALUES (1,'01'),(2,'02')");
        sqlite.ExecuteNonQuery(fixture.DestinationPath, $"CREATE TABLE ConversionRows (Id INTEGER PRIMARY KEY, {destinationColumn} TEXT)");
        var conversions = new Dictionary<string, DbaTableCopyColumnType>(mode == 2 ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal);
        if (mode == 0 || mode == 2) conversions["text"] = DbaTableCopyColumnType.Int32;
        if (mode == 1)
        {
            conversions["Text"] = DbaTableCopyColumnType.Int32;
            conversions["CopiedText"] = DbaTableCopyColumnType.String;
        }
        if (mode == 3)
        {
            conversions["Text"] = DbaTableCopyColumnType.Int32;
            conversions["text"] = DbaTableCopyColumnType.String;
        }
        if (mode == 4) conversions["Text"] = DbaTableCopyColumnType.Int32;
        var definition = new DbaTableCopyDefinition("ConversionRows", "ConversionRows", new[] { "Id" },
            ColumnMappings: mode is 1 or 2 ? new Dictionary<string, string>(StringComparer.Ordinal) { ["Text"] = "CopiedText" } : null,
            ColumnTypeConversions: conversions) { UseKeysetPagination = true };
        var options = new DbaTableCopyOptions { VerifyContent = true, CheckpointId = "conversion", PageSize = 1 };
        DbaTableCopyResult copied = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination, new[] { definition }, options);
        Assert.True(copied.Verified);
        Assert.Equal(mode == 0 ? "01" : "1", sqlite.ExecuteScalar(fixture.DestinationPath, $"SELECT {destinationColumn} FROM ConversionRows WHERE Id=1"));
        DbaTableCopyResult resumed = await new DbaTableCopyEngine().CopyAsync(fixture.Source, fixture.Destination, new[] { definition }, new() { VerifyContent = true, CheckpointId = "conversion", Resume = true });
        Assert.True(resumed.Verified);
        Assert.Equal(2, Assert.Single(resumed.Tables).ResumedRows);
    }
}
