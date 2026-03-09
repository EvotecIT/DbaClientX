using System.Collections.Generic;
using System.Collections.ObjectModel;
using DBAClientX.Mapping;

namespace DbaClientX.Tests;

public class DbPropertyAccessorTests
{
    private sealed class TestItem
    {
        public NestedItem User { get; init; } = new();
        public List<string> Tags { get; init; } = new();
    }

    private sealed class NestedItem
    {
        public string? Name { get; init; }
    }

    [Fact]
    public void TryGetValue_ReturnsFalse_WhenPathDoesNotExist()
    {
        var item = new TestItem { User = new NestedItem { Name = "Alice" } };

        var found = DbPropertyAccessor.TryGetValue(item, "User.Nmae", out var value);

        Assert.False(found);
        Assert.Null(value);
    }

    [Fact]
    public void TryGetValue_SupportsListIndexes()
    {
        var item = new TestItem { Tags = new List<string> { "first", "second" } };

        var found = DbPropertyAccessor.TryGetValue(item, "Tags.1", out var value);

        Assert.True(found);
        Assert.Equal("second", value);
    }

    [Fact]
    public void TryGetValue_ReturnsFalse_ForOutOfRangeListIndex()
    {
        var item = new TestItem { Tags = new List<string> { "first" } };

        var found = DbPropertyAccessor.TryGetValue(item, "Tags.9", out var value);

        Assert.False(found);
        Assert.Null(value);
    }

    [Fact]
    public void TryGetValue_IsCaseInsensitive_ForGenericStringDictionary()
    {
        var item = new Dictionary<string, object?>
        {
            ["user"] = new Dictionary<string, object?> { ["name"] = "Alice" }
        };

        var found = DbPropertyAccessor.TryGetValue(item, "User.Name", out var value);

        Assert.True(found);
        Assert.Equal("Alice", value);
    }

    [Fact]
    public void TryGetValue_SupportsReadOnlyStringDictionary()
    {
        IReadOnlyDictionary<string, object?> item = new ReadOnlyDictionary<string, object?>(
            new Dictionary<string, object?>
            {
                ["metadata"] = new ReadOnlyDictionary<string, object?>(
                    new Dictionary<string, object?> { ["runid"] = 42 })
            });

        var found = DbPropertyAccessor.TryGetValue(item, "Metadata.RunId", out var value);

        Assert.True(found);
        Assert.Equal(42, value);
    }
}
