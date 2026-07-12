using DBAClientX.DataMovement;

namespace DbaClientX.Tests;

public sealed class DbaIdentifierPathTests
{
    [Theory]
    [InlineData("schema.table", "schema", "table")]
    [InlineData("[schema.v1].[table.current]", "schema.v1", "table.current")]
    [InlineData("\"schema.v1\".\"table.current\"", "schema.v1", "table.current")]
    [InlineData("`schema.v1`.`table.current`", "schema.v1", "table.current")]
    public void SplitSegments_PreservesDelimitedDotsAndUnquotes(string path, string expectedSchema, string expectedTable)
    {
        var segments = DbaIdentifierPath.SplitSegments(path);

        Assert.Equal(2, segments.Count);
        Assert.Equal(expectedSchema, DbaIdentifierPath.UnquoteSegment(segments[0]));
        Assert.Equal(expectedTable, DbaIdentifierPath.UnquoteSegment(segments[1]));
    }

    [Theory]
    [InlineData("")]
    [InlineData("schema..table")]
    [InlineData("schema.")]
    [InlineData("\"unterminated")]
    public void SplitSegments_RejectsInvalidPaths(string path)
        => Assert.Throws<ArgumentException>(() => DbaIdentifierPath.SplitSegments(path));
}
