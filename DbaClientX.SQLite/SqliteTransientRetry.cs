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
        TransientRetry.Run(action, IsTransientSqlite, options, attempt => {
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
        return TransientRetry.Run(operation, IsTransientSqlite, options, attempt => {
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
        return TransientRetry.RunAsync(operation, IsTransientSqlite, options, attempt => {
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
        return TransientRetry.RunAsync(operation, IsTransientSqlite, options, attempt => {
            onRetry?.Invoke(attempt);
            onSqliteRetry?.Invoke(ToSqliteAttempt(attempt));
        }, cancellationToken);
    }

    private static bool IsTransientSqlite(Exception ex) =>
        ex is SqliteException sqliteEx &&
        sqliteEx.SqliteErrorCode is 5 or 6;

    private static SqliteTransientRetryAttempt ToSqliteAttempt(TransientRetryAttempt attempt) {
        var sqliteErrorCode = attempt.Exception is SqliteException sqlite ? sqlite.SqliteErrorCode : 0;
        return new SqliteTransientRetryAttempt(attempt.Attempt, attempt.Delay, sqliteErrorCode, attempt.Exception);
    }
}
