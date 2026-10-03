using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// Provides a common foundation for database client implementations, including
/// retry logic, parameter handling, and result materialization helpers.
/// </summary>
public abstract partial class DatabaseClientBase : IDisposable, IAsyncDisposable
{
    private readonly object _syncRoot = new();
    private ReturnType _returnType;
    private int _maxRetryAttempts = 3;
    private TimeSpan _retryDelay = TimeSpan.FromMilliseconds(200);
    private bool _retryNonQueryOperations;
    private int _disposeSignaled;

    private const int MaxBackoffMilliseconds = 30000; // cap backoff to 30s
    /// <summary>
    /// Gets or sets the desired return type for query executions.
    /// </summary>
    public ReturnType ReturnType
    {
        get { lock (_syncRoot) { return _returnType; } }
        set { lock (_syncRoot) { _returnType = value; } }
    }

    /// <summary>
    /// Gets or sets the maximum number of retry attempts for transient failures.
    /// A value lower than <c>1</c> is treated as a single attempt.
    /// </summary>
    public int MaxRetryAttempts
    {
        get { lock (_syncRoot) { return _maxRetryAttempts; } }
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxRetryAttempts cannot be negative.");
            }
            lock (_syncRoot) { _maxRetryAttempts = value; }
        }
    }

    /// <summary>
    /// Gets or sets the base delay between retry attempts. Exponential backoff
    /// with jitter is derived from this value.
    /// </summary>
    public TimeSpan RetryDelay
    {
        get { lock (_syncRoot) { return _retryDelay; } }
        set
        {
            if (value < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "RetryDelay cannot be negative.");
            }

            lock (_syncRoot) { _retryDelay = value; }
        }
    }

    /// <summary>
    /// Gets or sets whether mutating commands such as INSERT/UPDATE/DELETE should use automatic retries.
    /// Defaults to <see langword="false"/> to avoid replaying non-idempotent writes after partial success.
    /// This legacy option affects nonquery commands only; use <see cref="CommandRetryMode"/> for
    /// an explicit policy that also covers result-returning writes. Active transaction commands are never replayed.
    /// </summary>
    public bool RetryNonQueryOperations
    {
        get { lock (_syncRoot) { return _retryNonQueryOperations; } }
        set { lock (_syncRoot) { _retryNonQueryOperations = value; } }
    }

    /// <summary>
    /// Determines whether an exception represents a transient failure that warrants a retry.
    /// </summary>
    /// <param name="ex">The exception encountered during execution.</param>
    /// <returns><c>true</c> when the exception is transient; otherwise, <c>false</c>.</returns>
    protected virtual bool IsTransient(Exception ex) => false;

    /// <summary>
    /// Executes an operation with retry logic for transient failures.
    /// </summary>
    /// <typeparam name="T">The type of the result produced by the operation.</typeparam>
    /// <param name="operation">The operation to execute.</param>
    /// <returns>The result of the successful operation.</returns>
    /// <exception cref="Exception">Thrown when all retry attempts fail.</exception>
    protected T ExecuteWithRetry<T>(Func<T> operation)
        => TransientRetry.Run(operation, IsTransient, CreateTransientRetryOptions());

    /// <summary>
    /// Asynchronously executes an operation with retry logic for transient failures.
    /// </summary>
    /// <typeparam name="T">The type of the result produced by the operation.</typeparam>
    /// <param name="operation">The asynchronous operation to execute.</param>
    /// <param name="cancellationToken">Token used to cancel the retries.</param>
    /// <returns>The result of the successful operation.</returns>
    /// <exception cref="Exception">Thrown when all retry attempts fail.</exception>
    protected Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> operation, CancellationToken cancellationToken = default)
        => TransientRetry.RunAsync(
            _ => operation(),
            IsTransient,
            CreateTransientRetryOptions(),
            cancellationToken: cancellationToken);

    /// <summary>
    /// Computes the capped exponential backoff delay with jitter for a retry attempt.
    /// </summary>
    /// <param name="attempt">The one-based retry attempt number.</param>
    /// <returns>The delay before the next retry attempt.</returns>
    protected TimeSpan ComputeBackoffDelay(int attempt)
        => TransientRetry.CalculateBackoffDelay(CreateTransientRetryOptions(), attempt);

    /// <summary>Creates the shared retry options used by core execution and streaming paths.</summary>
    protected virtual TransientRetryOptions CreateTransientRetryOptions()
        => new()
        {
            MaxAttempts = Math.Max(1, MaxRetryAttempts),
            BaseDelay = RetryDelay,
            MaxDelay = TimeSpan.FromMilliseconds(MaxBackoffMilliseconds)
        };

    /// <summary>
    /// Validates SQL text or stored procedure names passed to execution helpers.
    /// </summary>
    protected static void ValidateCommandText(string commandText, CommandType commandType = CommandType.Text)
    {
        if (!string.IsNullOrWhiteSpace(commandText))
        {
            return;
        }

        var parameterName = commandType == CommandType.StoredProcedure ? "procedure" : "query";
        var message = commandType == CommandType.StoredProcedure
            ? "Stored procedure name cannot be null or whitespace."
            : "Query text cannot be null or whitespace.";
        throw new ArgumentException(message, parameterName);
    }

    /// <summary>
    /// Disposes the instance and suppresses finalization.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposeSignaled, 1) != 0)
        {
            return;
        }

        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Asynchronously disposes the instance and suppresses finalization.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeSignaled, 1) != 0)
        {
            return;
        }

        await DisposeAsyncCore().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases managed resources asynchronously. Override to dispose async-aware state.
    /// </summary>
    protected virtual ValueTask DisposeAsyncCore()
    {
        Dispose(true);
        return default;
    }

    /// <summary>
    /// Releases managed resources. Override to dispose additional state.
    /// </summary>
    /// <param name="disposing">Indicates whether the method is invoked from <see cref="Dispose()"/>.</param>
    protected virtual void Dispose(bool disposing)
    {
    }
}
