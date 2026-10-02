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
    /// <summary>
    /// Asynchronously disposes a resource only when the current operation owns it.
    /// </summary>
    /// <typeparam name="TResource">The resource type.</typeparam>
    /// <param name="resource">The resource instance to dispose.</param>
    /// <param name="ownsResource"><see langword="true"/> when the caller created and owns the resource.</param>
    /// <param name="disposeAsync">Asynchronous disposal callback for the resource.</param>
    protected static async ValueTask DisposeOwnedResourceAsync<TResource>(TResource? resource, bool ownsResource, Func<TResource, ValueTask> disposeAsync)
        where TResource : class
    {
        if (!ownsResource || resource == null)
        {
            return;
        }

        await disposeAsync(resource).ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes both resources in order, retaining the primary failure if both disposers throw; skips <see langword="null"/> values.
    /// </summary>
    /// <typeparam name="TPrimary">The primary resource type.</typeparam>
    /// <typeparam name="TSecondary">The secondary resource type.</typeparam>
    /// <param name="primary">The primary resource instance.</param>
    /// <param name="disposePrimary">Synchronous disposal callback for the primary resource.</param>
    /// <param name="secondary">The secondary resource instance.</param>
    /// <param name="disposeSecondary">Synchronous disposal callback for the secondary resource.</param>
    protected static void DisposeResourcePair<TPrimary, TSecondary>(
        TPrimary? primary,
        Action<TPrimary> disposePrimary,
        TSecondary? secondary,
        Action<TSecondary> disposeSecondary)
        where TPrimary : class
        where TSecondary : class
    {
        Exception? primaryFailure = null;
        try
        {
            if (primary != null) disposePrimary(primary);
        }
        catch (Exception exception)
        {
            primaryFailure = exception;
            throw;
        }
        finally
        {
            try
            {
                if (secondary != null) disposeSecondary(secondary);
            }
            catch when (primaryFailure != null)
            {
                // Both resources were attempted. Preserve the original failure and its stack.
            }
        }
    }

    /// <summary>
    /// Asynchronously disposes both resources, retaining the primary failure if both disposers throw; skips <see langword="null"/> values.
    /// </summary>
    /// <typeparam name="TPrimary">The primary resource type.</typeparam>
    /// <typeparam name="TSecondary">The secondary resource type.</typeparam>
    /// <param name="primary">The primary resource instance.</param>
    /// <param name="disposePrimaryAsync">Asynchronous disposal callback for the primary resource.</param>
    /// <param name="secondary">The secondary resource instance.</param>
    /// <param name="disposeSecondaryAsync">Asynchronous disposal callback for the secondary resource.</param>
    protected static async ValueTask DisposeResourcePairAsync<TPrimary, TSecondary>(
        TPrimary? primary,
        Func<TPrimary, ValueTask> disposePrimaryAsync,
        TSecondary? secondary,
        Func<TSecondary, ValueTask> disposeSecondaryAsync)
        where TPrimary : class
        where TSecondary : class
    {
        Exception? primaryFailure = null;
        try
        {
            if (primary != null) await disposePrimaryAsync(primary).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            primaryFailure = exception;
            throw;
        }
        finally
        {
            try
            {
                if (secondary != null) await disposeSecondaryAsync(secondary).ConfigureAwait(false);
            }
            catch when (primaryFailure != null)
            {
                // Both resources were attempted. Preserve the original failure and its stack.
            }
        }
    }
}
