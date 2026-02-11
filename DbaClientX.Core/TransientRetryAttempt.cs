using System;

namespace DBAClientX;

/// <summary>
/// Describes a retry attempt triggered by <see cref="TransientRetry"/>.
/// </summary>
public sealed class TransientRetryAttempt {
    /// <summary>
    /// Initializes a new instance of the <see cref="TransientRetryAttempt"/> class.
    /// </summary>
    /// <param name="attempt">The 1-based attempt number that failed and triggered this retry.</param>
    /// <param name="delay">The delay that will be applied before the next attempt.</param>
    /// <param name="exception">The transient exception that caused the retry.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="attempt"/> is less than 1.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="exception"/> is <see langword="null"/>.</exception>
    public TransientRetryAttempt(int attempt, TimeSpan delay, Exception exception) {
        if (attempt < 1) {
            throw new ArgumentOutOfRangeException(nameof(attempt), "Attempt must be >= 1.");
        }

        Attempt = attempt;
        Delay = delay;
        Exception = exception ?? throw new ArgumentNullException(nameof(exception));
    }

    /// <summary>
    /// Gets the 1-based attempt number that failed and triggered this retry.
    /// </summary>
    public int Attempt { get; }

    /// <summary>
    /// Gets the delay that will be applied before the next attempt.
    /// </summary>
    public TimeSpan Delay { get; }

    /// <summary>
    /// Gets the transient exception that caused the retry.
    /// </summary>
    public Exception Exception { get; }
}
