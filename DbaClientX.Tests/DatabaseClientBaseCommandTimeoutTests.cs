using DBAClientX;
using Microsoft.Data.SqlClient;

namespace DbaClientX.Tests;

public class DatabaseClientBaseCommandTimeoutTests
{
    private sealed class TimeoutProbeClient : DatabaseClientBase
    {
        public int ApplyToCommandWithDefault(int providerDefault)
        {
            using var command = new SqlCommand
            {
                CommandTimeout = providerDefault
            };
            ApplyCommandTimeout(command);
            return command.CommandTimeout;
        }
    }

    [Fact]
    public void UnconfiguredTimeout_PreservesProviderDefault()
    {
        using var client = new TimeoutProbeClient();

        Assert.Equal(37, client.ApplyToCommandWithDefault(37));
    }

    [Fact]
    public void ExplicitZero_DisablesCommandTimeout()
    {
        using var client = new TimeoutProbeClient
        {
            CommandTimeout = 0
        };

        Assert.Equal(0, client.ApplyToCommandWithDefault(37));
    }

    [Fact]
    public void PositiveTimeout_OverridesProviderDefault()
    {
        using var client = new TimeoutProbeClient
        {
            CommandTimeout = 75
        };

        Assert.Equal(75, client.ApplyToCommandWithDefault(37));
    }

    [Fact]
    public void ResetCommandTimeout_RestoresProviderDefaultBehavior()
    {
        using var client = new TimeoutProbeClient
        {
            CommandTimeout = 0
        };

        client.ResetCommandTimeout();

        Assert.Equal(37, client.ApplyToCommandWithDefault(37));
    }

    [Fact]
    public void NegativeTimeout_IsRejected()
    {
        using var client = new TimeoutProbeClient();

        Assert.Throws<ArgumentOutOfRangeException>(() => client.CommandTimeout = -1);
    }
}
