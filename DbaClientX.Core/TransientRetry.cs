using System;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

/// <summary>
/// Provides reusable sync and async retry execution for transient failures.
/// </summary>
public static class TransientRetry {
    private static readonly ThreadLocal<Random> RandomProvider = new(
        () => new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId)));

    /// <summary>
    /// Executes an operation with transient retry behavior.
    /// </summary>
    /// <param name="action">Operation to execute.</param>
    /// <param name="isTransient">Predicate used to classify transient exceptions.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    public static void Run(
        Action action,
        Func<Exception, bool> isTransient,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null) {
        if (action is null) {
            throw new ArgumentNullException(nameof(action));
        }

        Run(() => {
            action();
            return 0;
        }, isTransient, options, onRetry);
    }

    /// <summary>
    /// Executes an operation with transient retry behavior.
    /// </summary>
    /// <typeparam name="T">Result type produced by the operation.</typeparam>
    /// <param name="operation">Operation to execute.</param>
    /// <param name="isTransient">Predicate used to classify transient exceptions.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <returns>The successful operation result.</returns>
    public static T Run<T>(
        Func<T> operation,
        Func<Exception, bool> isTransient,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null) {
        if (operation is null) {
            throw new ArgumentNullException(nameof(operation));
        }
        if (isTransient is null) {
            throw new ArgumentNullException(nameof(isTransient));
        }

        options ??= TransientRetryOptions.Default;
        Exception? last = null;

        for (var attempt = 1; attempt <= options.MaxAttempts; attempt++) {
            try {
                return operation();
            } catch (Exception ex) when (isTransient(ex)) {
                last = ex;
                if (attempt >= options.MaxAttempts) {
                    break;
                }

                var delay = ComputeBackoffDelay(options, attempt);
                onRetry?.Invoke(new TransientRetryAttempt(attempt, delay, ex));
                if (delay > TimeSpan.Zero) {
                    Thread.Sleep(delay);
                }
            }
        }

        throw last ?? new InvalidOperationException("Operation failed.");
    }

    /// <summary>
    /// Asynchronously executes an operation with transient retry behavior.
    /// </summary>
    /// <param name="operation">Operation to execute.</param>
    /// <param name="isTransient">Predicate used to classify transient exceptions.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <param name="cancellationToken">Token used to cancel retries and delays.</param>
    public static async Task RunAsync(
        Func<CancellationToken, Task> operation,
        Func<Exception, bool> isTransient,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null,
        CancellationToken cancellationToken = default) {
        if (operation is null) {
            throw new ArgumentNullException(nameof(operation));
        }

        await RunAsync(
            async ct => {
                await operation(ct).ConfigureAwait(false);
                return 0;
            },
            isTransient,
            options,
            onRetry,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Asynchronously executes an operation with transient retry behavior.
    /// </summary>
    /// <typeparam name="T">Result type produced by the operation.</typeparam>
    /// <param name="operation">Operation to execute.</param>
    /// <param name="isTransient">Predicate used to classify transient exceptions.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <param name="cancellationToken">Token used to cancel retries and delays.</param>
    /// <returns>The successful operation result.</returns>
    public static async Task<T> RunAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        Func<Exception, bool> isTransient,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null,
        CancellationToken cancellationToken = default) {
        if (operation is null) {
            throw new ArgumentNullException(nameof(operation));
        }
        if (isTransient is null) {
            throw new ArgumentNullException(nameof(isTransient));
        }

        options ??= TransientRetryOptions.Default;
        Exception? last = null;

        for (var attempt = 1; attempt <= options.MaxAttempts; attempt++) {
            cancellationToken.ThrowIfCancellationRequested();

            try {
                return await operation(cancellationToken).ConfigureAwait(false);
            } catch (Exception ex) when (isTransient(ex)) {
                last = ex;
                if (attempt >= options.MaxAttempts) {
                    break;
                }

                var delay = ComputeBackoffDelay(options, attempt);
                onRetry?.Invoke(new TransientRetryAttempt(attempt, delay, ex));

                if (delay > TimeSpan.Zero) {
                    if (options.DelayAsync is null) {
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    } else {
                        await options.DelayAsync(delay, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        throw last ?? new InvalidOperationException("Operation failed.");
    }

    private static TimeSpan ComputeBackoffDelay(TransientRetryOptions options, int attempt) {
        if (options.BaseDelay <= TimeSpan.Zero || options.MaxDelay <= TimeSpan.Zero) {
            return TimeSpan.Zero;
        }

        var baseMs = options.BaseDelay.TotalMilliseconds;
        var maxMs = options.MaxDelay.TotalMilliseconds;
        var factor = Math.Pow(2, Math.Max(0, attempt - 1));
        var backoffMs = Math.Min(maxMs, baseMs * factor);
        var jitterFactor = options.JitterFactorProvider?.Invoke(attempt) ?? RandomProvider.Value!.NextDouble();
        jitterFactor = Clamp01(jitterFactor);
        var jitterMs = baseMs * jitterFactor;
        var totalMs = Math.Min(maxMs, backoffMs + jitterMs);
        return TimeSpan.FromMilliseconds(totalMs);
    }

    private static double Clamp01(double value) {
        if (double.IsNaN(value) || double.IsInfinity(value)) {
            return 0;
        }

        if (value < 0) {
            return 0;
        }

        if (value > 1) {
            return 1;
        }

        return value;
    }
}
