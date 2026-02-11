using System;

namespace DBAClientX;

/// <summary>
/// Describes a SQLite-specific retry attempt emitted by <see cref="SqliteTransientRetry"/>.
/// </summary>
public sealed class SqliteTransientRetryAttempt {
    /// <summary>
    /// Initializes a new instance of the <see cref="SqliteTransientRetryAttempt"/> class.
    /// </summary>
    /// <param name="attempt">The 1-based attempt number that failed and triggered this retry.</param>
    /// <param name="delay">The delay that will be applied before the next attempt.</param>
    /// <param name="sqliteErrorCode">The SQLite error code associated with the transient failure.</param>
    /// <param name="exception">The originating SQLite exception.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="attempt"/> is less than 1.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="exception"/> is <see langword="null"/>.</exception>
    public SqliteTransientRetryAttempt(int attempt, TimeSpan delay, int sqliteErrorCode, Exception exception) {
        if (attempt < 1) {
            throw new ArgumentOutOfRangeException(nameof(attempt), "Attempt must be >= 1.");
        }

        Attempt = attempt;
        Delay = delay;
        SqliteErrorCode = sqliteErrorCode;
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
    /// Gets the SQLite error code associated with the transient failure.
    /// </summary>
    public int SqliteErrorCode { get; }

    /// <summary>
    /// Gets the originating SQLite exception.
    /// </summary>
    public Exception Exception { get; }
}
