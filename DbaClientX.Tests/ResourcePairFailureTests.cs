using DBAClientX;

namespace DbaClientX.Tests;

public sealed class ResourcePairFailureTests
{
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task DisposalFailure_AttemptsBothResourcesAndPreservesFirstFailure(bool asynchronous, bool primaryFails)
    {
        var primaryFailure = new InvalidOperationException("primary");
        var secondaryFailure = new InvalidOperationException("secondary");
        var disposed = new List<string>();
        using var client = new ResourceClient();
        void Primary(object resource)
        {
            disposed.Add("primary");
            if (primaryFails) throw primaryFailure;
        }
        void Secondary(object resource)
        {
            disposed.Add("secondary");
            throw secondaryFailure;
        }

        Exception failure;
        if (asynchronous)
            failure = await Assert.ThrowsAsync<InvalidOperationException>(() => client.DisposePairAsync(Primary, Secondary));
        else
            failure = Assert.Throws<InvalidOperationException>(() => client.DisposePair(Primary, Secondary));

        Assert.Equal(new[] { "primary", "secondary" }, disposed);
        Assert.Same(primaryFails ? primaryFailure : secondaryFailure, failure);
    }

    private sealed class ResourceClient : DatabaseClientBase
    {
        public void DisposePair(Action<object> primary, Action<object> secondary)
            => DisposeResourcePair(new object(), primary, new object(), secondary);

        public async Task DisposePairAsync(Action<object> primary, Action<object> secondary)
            => await DisposeResourcePairAsync(new object(), resource => { primary(resource); return default; },
                new object(), resource => { secondary(resource); return default; });
    }
}
