using System;
using System.Collections.Generic;
using DBAClientX.Mapping;

namespace DbaClientX.Tests;

public class DbParameterMapperTests
{
    private sealed class NestedItem
    {
        public string? Name { get; init; }
    }

    private sealed class TestItem
    {
        public string? RunId { get; init; }
        public NestedItem? User { get; init; }
    }

    [Fact]
    public void MapItem_PrefersItemValues_OverAmbientValues()
    {
        var item = new TestItem { RunId = "item-run-id" };
        var map = new Dictionary<string, string> { ["RunId"] = "@RunId" };
        var ambient = new Dictionary<string, object?> { ["RunId"] = "ambient-run-id" };

        var result = DbParameterMapper.MapItem(item, map, ambient: ambient);

        Assert.Equal("item-run-id", result["@RunId"]);
    }

    [Fact]
    public void MapItem_UsesAmbientValues_WhenItemDoesNotProvideValue()
    {
        var item = new TestItem { User = new NestedItem { Name = "Alice" } };
        var map = new Dictionary<string, string>
        {
            ["User.Name"] = "@UserName",
            ["RunId"] = "@RunId"
        };
        var ambient = new Dictionary<string, object?> { ["RunId"] = "ambient-run-id" };

        var result = DbParameterMapper.MapItem(item, map, ambient: ambient);

        Assert.Equal("Alice", result["@UserName"]);
        Assert.Equal("ambient-run-id", result["@RunId"]);
    }

    [Fact]
    public void MapItem_UsesAmbientValues_CaseInsensitively()
    {
        var map = new Dictionary<string, string> { ["RunId"] = "@RunId" };
        var ambient = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["runid"] = "ambient-run-id"
        };

        var result = DbParameterMapper.MapItem(item: null, map, ambient: ambient);

        Assert.Equal("ambient-run-id", result["@RunId"]);
    }

    [Fact]
    public void MapItem_ConvertsDateTimeOffsetAmbientValues_ToUtcDateTime()
    {
        var timestamp = new DateTimeOffset(2026, 3, 9, 12, 30, 0, TimeSpan.FromHours(2));
        var map = new Dictionary<string, string> { ["TsUtc"] = "@TsUtc" };
        var ambient = new Dictionary<string, object?> { ["TsUtc"] = timestamp };

        var result = DbParameterMapper.MapItem(item: null, map, ambient: ambient);

        Assert.Equal(timestamp.UtcDateTime, result["@TsUtc"]);
    }
}
