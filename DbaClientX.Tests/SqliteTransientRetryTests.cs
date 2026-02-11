using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX;
using Microsoft.Data.Sqlite;

namespace DbaClientX.Tests;

public class SqliteTransientRetryTests {
    [Fact]
    public void Run_RetriesBusyErrors() {
        var attempts = 0;
        var options = new TransientRetryOptions {
            MaxAttempts = 3,
            BaseDelay = TimeSpan.Zero
        };

        var result = SqliteTransientRetry.Run(
            () => {
                attempts++;
                if (attempts < 3) {
                    throw new SqliteException("busy", 5);
                }
                return 9;
            },
            options);

        Assert.Equal(9, result);
        Assert.Equal(3, attempts);
    }

    [Fact]
    public void Run_DoesNotRetryNonTransientErrors() {
        var attempts = 0;
        var options = new TransientRetryOptions {
            MaxAttempts = 3,
            BaseDelay = TimeSpan.Zero
        };

        Assert.Throws<SqliteException>(() =>
            SqliteTransientRetry.Run<int>(
                () => {
                    attempts++;
                    throw new SqliteException("not-transient", 1);
                },
                options));

        Assert.Equal(1, attempts);
    }

    [Fact]
    public async Task RunAsync_RetriesLockedErrors() {
        var attempts = 0;
        var options = new TransientRetryOptions {
            MaxAttempts = 2,
            BaseDelay = TimeSpan.Zero
        };

        var result = await SqliteTransientRetry.RunAsync(
            _ => {
                attempts++;
                if (attempts < 2) {
                    throw new SqliteException("locked", 6);
                }
                return Task.FromResult("ok");
            },
            options,
            cancellationToken: CancellationToken.None);

        Assert.Equal("ok", result);
        Assert.Equal(2, attempts);
    }

    [Fact]
    public void Run_EmitsSqliteRetryTelemetryWithErrorCode() {
        var attempts = 0;
        var telemetry = new List<SqliteTransientRetryAttempt>();
        var options = new TransientRetryOptions {
            MaxAttempts = 3,
            BaseDelay = TimeSpan.FromMilliseconds(10),
            MaxDelay = TimeSpan.FromMilliseconds(100),
            JitterFactorProvider = _ => 0
        };

        var result = SqliteTransientRetry.Run(
            () => {
                attempts++;
                if (attempts < 3) {
                    throw new SqliteException("busy", 5);
                }
                return 21;
            },
            options,
            onSqliteRetry: telemetry.Add);

        Assert.Equal(21, result);
        Assert.Equal(3, attempts);
        Assert.Equal(2, telemetry.Count);
        Assert.Equal(1, telemetry[0].Attempt);
        Assert.Equal(TimeSpan.FromMilliseconds(10), telemetry[0].Delay);
        Assert.Equal(5, telemetry[0].SqliteErrorCode);
        Assert.Equal(2, telemetry[1].Attempt);
        Assert.Equal(TimeSpan.FromMilliseconds(20), telemetry[1].Delay);
        Assert.Equal(5, telemetry[1].SqliteErrorCode);
    }
}
