using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;

namespace DbaClientX.Tests;

public class TransientRetryTests {
    private sealed class RetryableTestException : Exception { }

    [Fact]
    public void Run_RetriesUntilSuccess() {
        var attempts = 0;
        var observed = new List<TransientRetryAttempt>();
        var options = new TransientRetryOptions {
            MaxAttempts = 3,
            BaseDelay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(100),
            JitterFactorProvider = _ => 0
        };

        var result = TransientRetry.Run(
            () => {
                attempts++;
                if (attempts < 3) {
                    throw new RetryableTestException();
                }
                return 42;
            },
            static ex => ex is RetryableTestException,
            options,
            observed.Add);

        Assert.Equal(42, result);
        Assert.Equal(3, attempts);
        Assert.Equal(2, observed.Count);
        Assert.Equal(1, observed[0].Attempt);
        Assert.Equal(TimeSpan.FromMilliseconds(10), observed[0].Delay);
        Assert.Equal(2, observed[1].Attempt);
        Assert.Equal(TimeSpan.FromMilliseconds(20), observed[1].Delay);
    }

    [Fact]
    public void Run_ThrowsAfterMaxAttempts() {
        var attempts = 0;
        var options = new TransientRetryOptions {
            MaxAttempts = 2,
            BaseDelay = TimeSpan.Zero
        };

        Assert.Throws<RetryableTestException>(() =>
            TransientRetry.Run(
                () => {
                    attempts++;
                    throw new RetryableTestException();
                },
                static ex => ex is RetryableTestException,
                options));

        Assert.Equal(2, attempts);
    }

    [Fact]
    public async Task RunAsync_UsesDelayHookAndRetries() {
        var attempts = 0;
        var delays = new List<TimeSpan>();
        var options = new TransientRetryOptions {
            MaxAttempts = 3,
            BaseDelay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(100),
            JitterFactorProvider = _ => 0,
            DelayAsync = (delay, token) => {
                delays.Add(delay);
                return Task.CompletedTask;
            }
        };

        var result = await TransientRetry.RunAsync(
            _ => {
                attempts++;
                if (attempts < 3) {
                    throw new RetryableTestException();
                }
                return Task.FromResult(7);
            },
            static ex => ex is RetryableTestException,
            options);

        Assert.Equal(7, result);
        Assert.Equal(3, attempts);
        Assert.Equal(new[] { TimeSpan.FromMilliseconds(10), TimeSpan.FromMilliseconds(20) }, delays);
    }

    [Fact]
    public async Task RunAsync_HonorsCancellation() {
        var options = new TransientRetryOptions {
            MaxAttempts = 3,
            BaseDelay = TimeSpan.FromMilliseconds(10)
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            TransientRetry.RunAsync(
                _ => Task.FromResult(1),
                static _ => true,
                options,
                cancellationToken: cts.Token));
    }
}
