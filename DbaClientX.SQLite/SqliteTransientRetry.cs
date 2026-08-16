using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// Provides SQLite-specific transient retry execution for busy/locked errors.
/// </summary>
public static class SqliteTransientRetry {
    /// <summary>
    /// Executes an operation with SQLite transient retry behavior.
    /// </summary>
    /// <param name="action">Operation to execute.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <param name="onSqliteRetry">Optional SQLite-specific callback that includes error code details.</param>
    public static void Run(
        Action action,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null,
        Action<SqliteTransientRetryAttempt>? onSqliteRetry = null) {
        TransientRetry.Run(action, IsTransient, options, attempt => {
            onRetry?.Invoke(attempt);
            onSqliteRetry?.Invoke(ToSqliteAttempt(attempt));
        });
    }

    /// <summary>
    /// Executes an operation with SQLite transient retry behavior.
    /// </summary>
    /// <typeparam name="T">Result type produced by the operation.</typeparam>
    /// <param name="operation">Operation to execute.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <param name="onSqliteRetry">Optional SQLite-specific callback that includes error code details.</param>
    /// <returns>The successful operation result.</returns>
    public static T Run<T>(
        Func<T> operation,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null,
        Action<SqliteTransientRetryAttempt>? onSqliteRetry = null) {
        return TransientRetry.Run(operation, IsTransient, options, attempt => {
            onRetry?.Invoke(attempt);
            onSqliteRetry?.Invoke(ToSqliteAttempt(attempt));
        });
    }

    /// <summary>
    /// Asynchronously executes an operation with SQLite transient retry behavior.
    /// </summary>
    /// <param name="operation">Operation to execute.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <param name="onSqliteRetry">Optional SQLite-specific callback that includes error code details.</param>
    /// <param name="cancellationToken">Token used to cancel retries and delays.</param>
    public static Task RunAsync(
        Func<CancellationToken, Task> operation,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null,
        Action<SqliteTransientRetryAttempt>? onSqliteRetry = null,
        CancellationToken cancellationToken = default) {
        return TransientRetry.RunAsync(operation, IsTransient, options, attempt => {
            onRetry?.Invoke(attempt);
            onSqliteRetry?.Invoke(ToSqliteAttempt(attempt));
        }, cancellationToken);
    }

    /// <summary>
    /// Asynchronously executes an operation with SQLite transient retry behavior.
    /// </summary>
    /// <typeparam name="T">Result type produced by the operation.</typeparam>
    /// <param name="operation">Operation to execute.</param>
    /// <param name="options">Retry options. When <see langword="null"/>, defaults are used.</param>
    /// <param name="onRetry">Optional callback invoked before each delay/retry.</param>
    /// <param name="onSqliteRetry">Optional SQLite-specific callback that includes error code details.</param>
    /// <param name="cancellationToken">Token used to cancel retries and delays.</param>
    /// <returns>The successful operation result.</returns>
    public static Task<T> RunAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        TransientRetryOptions? options = null,
        Action<TransientRetryAttempt>? onRetry = null,
        Action<SqliteTransientRetryAttempt>? onSqliteRetry = null,
        CancellationToken cancellationToken = default) {
        return TransientRetry.RunAsync(operation, IsTransient, options, attempt => {
            onRetry?.Invoke(attempt);
            onSqliteRetry?.Invoke(ToSqliteAttempt(attempt));
        }, cancellationToken);
    }

    /// <summary>
    /// Determines whether an exception chain contains a retryable SQLite busy, locked, or I/O failure.
    /// </summary>
    /// <param name="exception">The exception to classify.</param>
    /// <returns><see langword="true"/> when the failure is suitable for bounded retry.</returns>
    public static bool IsTransient(Exception exception) {
        if (exception is null) {
            throw new ArgumentNullException(nameof(exception));
        }

        return FindSqliteException(exception) is not null;
    }

    private static SqliteTransientRetryAttempt ToSqliteAttempt(TransientRetryAttempt attempt) {
        int sqliteErrorCode = FindSqliteException(attempt.Exception)?.SqliteErrorCode ?? 0;
        return new SqliteTransientRetryAttempt(attempt.Attempt, attempt.Delay, sqliteErrorCode, attempt.Exception);
    }

    private static SqliteException? FindSqliteException(Exception exception) {
        for (Exception? current = exception; current != null; current = current.InnerException) {
            if (current is SqliteException sqliteException &&
                sqliteException.SqliteErrorCode is 5 or 6 or 10) {
                return sqliteException;
            }
        }

        return null;
    }
}
