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

public abstract partial class DatabaseClientBase
{
    /// <summary>Returns whether an exception carries the caller's cancellation token.</summary>
    protected static bool IsCallerCancellation(Exception exception, CancellationToken cancellationToken)
        => cancellationToken.IsCancellationRequested &&
           exception is OperationCanceledException cancellationException &&
           cancellationException.CancellationToken == cancellationToken;

    /// <summary>Returns whether an exception is a provider-specific representation of command cancellation.</summary>
    protected virtual bool IsProviderCancellationException(Exception exception)
        => false;

    /// <summary>Searches an exception and its inner-exception chain for a matching provider failure.</summary>
    protected static bool ExceptionChainContains<TException>(
        Exception exception,
        Func<TException, bool> predicate)
        where TException : Exception
    {
        if (predicate == null)
        {
            throw new ArgumentNullException(nameof(predicate));
        }

        for (Exception? current = exception; current != null; current = current.InnerException)
        {
            if (current is TException candidate && predicate(candidate))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Creates the public exception for a failed asynchronous database operation while preserving caller cancellation.
    /// </summary>
    /// <remarks>
    /// Some ADO.NET providers report a canceled command as a provider exception instead of
    /// <see cref="OperationCanceledException"/>. Once the caller's token is canceled, normalize that provider-specific
    /// failure back to the standard cancellation contract and retain only sanitized provider context.
    /// </remarks>
    protected Exception CreateQueryExecutionOrCancellationException(
        string message,
        string commandText,
        Exception exception,
        CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested && IsProviderCancellationException(exception))
        {
            return CreateCallerCancellationException(exception, cancellationToken);
        }

        return CreateQueryExecutionException(message, commandText, exception);
    }

    /// <summary>Creates a query failure that retains only safe provider classification metadata.</summary>
    protected DbaQueryExecutionException CreateQueryExecutionException(
        string message,
        string commandText,
        Exception exception)
        => new(
            message,
            commandText,
            exception,
            GetProviderErrorCode(exception),
            GetProviderSqlState(exception),
            GetProviderErrorKind(exception));

    /// <summary>Returns a provider-native numeric error code suitable for public diagnostics.</summary>
    protected virtual int? GetProviderErrorCode(Exception exception)
        => exception is DbaQueryExecutionException queryException
            ? queryException.ProviderErrorCode
            : (exception as System.Data.Common.DbException)?.ErrorCode;

    /// <summary>Returns a provider SQLSTATE suitable for public diagnostics.</summary>
    protected virtual string? GetProviderSqlState(Exception exception)
        => exception is DbaQueryExecutionException queryException
            ? queryException.ProviderSqlState
            : null;

    /// <summary>Returns a portable provider failure category suitable for public diagnostics.</summary>
    protected virtual DbaProviderErrorKind GetProviderErrorKind(Exception exception)
        => exception is DbaQueryExecutionException queryException
            ? queryException.ProviderErrorKind
            : DbaProviderErrorKind.Unknown;

    /// <summary>Creates a standard caller-cancellation exception while retaining only sanitized provider context.</summary>
    protected static OperationCanceledException CreateCallerCancellationException(
        Exception exception,
        CancellationToken cancellationToken)
        => new(
            "The database operation was canceled by the caller.",
            DbaQueryExecutionException.CreateSanitizedProviderException(exception),
            cancellationToken);

    /// <summary>Awaits an ADO.NET operation and normalizes provider-specific cancellation failures.</summary>
    protected async Task<T> AwaitWithCallerCancellationAsync<T>(
        Func<Task<T>> operation,
        CancellationToken cancellationToken)
    {
        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (
            !IsCallerCancellation(ex, cancellationToken) &&
            cancellationToken.IsCancellationRequested)
        {
            // This wrapper encloses only the provider await, so an unassociated OCE still has
            // provider provenance. Do not apply this inference around user callbacks.
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
        catch (Exception ex) when (
            cancellationToken.IsCancellationRequested &&
            IsProviderCancellationException(ex))
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
    }

    /// <summary>Awaits a non-result ADO.NET operation and normalizes provider-specific cancellation failures.</summary>
    protected async Task AwaitWithCallerCancellationAsync(
        Func<Task> operation,
        CancellationToken cancellationToken)
    {
        try
        {
            await operation().ConfigureAwait(false);
        }
        catch (OperationCanceledException ex) when (
            !IsCallerCancellation(ex, cancellationToken) &&
            cancellationToken.IsCancellationRequested)
        {
            // This wrapper encloses only the provider await, so an unassociated OCE still has
            // provider provenance. Do not apply this inference around user callbacks.
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
        catch (Exception ex) when (
            cancellationToken.IsCancellationRequested &&
            IsProviderCancellationException(ex))
        {
            throw CreateCallerCancellationException(ex, cancellationToken);
        }
    }
}
