using System;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

/// <summary>
/// Configures retry behavior used by <see cref="TransientRetry"/>.
/// </summary>
public sealed class TransientRetryOptions {
    /// <summary>
    /// Gets default options matching core retry behavior.
    /// </summary>
    public static TransientRetryOptions Default => new();

    private int _maxAttempts = 3;
    private TimeSpan _baseDelay = TimeSpan.FromMilliseconds(200);
    private TimeSpan _maxDelay = TimeSpan.FromMilliseconds(30000);

    /// <summary>
    /// Gets or sets the maximum number of attempts, including the first try.
    /// </summary>
    /// <remarks>
    /// Set to <c>1</c> to disable retries.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is less than 1.</exception>
    public int MaxAttempts {
        get => _maxAttempts;
        set {
            if (value < 1) {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxAttempts must be >= 1.");
            }
            _maxAttempts = value;
        }
    }

    /// <summary>
    /// Gets or sets the base delay used for exponential backoff.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    public TimeSpan BaseDelay {
        get => _baseDelay;
        set {
            if (value < TimeSpan.Zero) {
                throw new ArgumentOutOfRangeException(nameof(value), "BaseDelay cannot be negative.");
            }
            _baseDelay = value;
        }
    }

    /// <summary>
    /// Gets or sets the maximum backoff delay.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value is negative.</exception>
    public TimeSpan MaxDelay {
        get => _maxDelay;
        set {
            if (value < TimeSpan.Zero) {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxDelay cannot be negative.");
            }
            _maxDelay = value;
        }
    }

    /// <summary>
    /// Gets or sets an optional jitter provider for deterministic testing.
    /// </summary>
    /// <remarks>
    /// The delegate receives the current failed attempt number (1-based) and should return a factor in range <c>[0,1]</c>.
    /// Values outside the range are clamped.
    /// </remarks>
    public Func<int, double>? JitterFactorProvider { get; set; }

    /// <summary>
    /// Gets or sets an optional async delay hook.
    /// </summary>
    /// <remarks>
    /// When not provided, <see cref="Task.Delay(TimeSpan, CancellationToken)"/> is used.
    /// </remarks>
    public Func<TimeSpan, CancellationToken, Task>? DelayAsync { get; set; }
}
