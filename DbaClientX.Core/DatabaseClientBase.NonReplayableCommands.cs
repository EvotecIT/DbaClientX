using System.Data.Common;
using DBAClientX.Diagnostics;

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    /// <summary>Bounded diagnostic operations supported by native commands that must never replay.</summary>
    protected enum NonReplayableCommandKind
    {
        /// <summary>A command that returns an affected-row count.</summary>
        NonQuery,
        /// <summary>A command that returns a scalar value.</summary>
        Scalar,
        /// <summary>A reader-open operation; later consumption belongs to its caller.</summary>
        ReaderOpen
    }

    /// <summary>Invokes a native provider command once, preserving cancellation and optional execution diagnostics.</summary>
    /// <typeparam name="T">The native command result.</typeparam>
    /// <param name="operation">One native command invocation; it is never replayed.</param>
    /// <param name="connection">The connection owned by the calling operation.</param>
    /// <param name="query">The statement used only for an observed fingerprint.</param>
    /// <param name="cancellationToken">The caller token supplied to the provider operation.</param>
    /// <param name="kind">The bounded operation used for diagnostics.</param>
    /// <param name="countRows">Optional successful row accounting; reader consumption remains caller-owned.</param>
    /// <returns>The provider result from the single invocation.</returns>
    /// <remarks>
    /// Administrative operations such as restore cannot safely replay after an uncertain outcome. This method
    /// ignores command replay settings; connection establishment retains its separate policy. Callers retain
    /// connection/reader ownership and apply their public error-redaction boundary.
    /// </remarks>
    protected Task<T> ExecuteNonReplayableCommandAsync<T>(
        Func<Task<T>> operation, DbConnection connection, string query, CancellationToken cancellationToken,
        NonReplayableCommandKind kind = NonReplayableCommandKind.NonQuery, Func<T, long?>? countRows = null)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return !DbaClientXDiagnostics.IsCommandObserved
            ? AwaitWithCallerCancellationAsync(operation, cancellationToken)
            : ExecuteObservedNonReplayableCommandAsync(operation, connection, query, cancellationToken,
                kind, countRows);
    }

    private async Task<T> ExecuteObservedNonReplayableCommandAsync<T>(
        Func<Task<T>> operation, DbConnection connection, string query, CancellationToken cancellationToken,
        NonReplayableCommandKind kind, Func<T, long?>? countRows)
    {
        string operationName = kind switch
        {
            NonReplayableCommandKind.Scalar => "command.scalar",
            NonReplayableCommandKind.ReaderOpen => "reader.open",
            _ => "command.nonquery"
        };
        using var scope = DbaClientXDiagnostics.StartCommand(connection, query, operationName);
        try
        {
            T result = await AwaitWithCallerCancellationAsync(operation, cancellationToken).ConfigureAwait(false);
            scope.Complete(countRows?.Invoke(result));
            return result;
        }
        catch (Exception exception) { scope.Fail(exception, cancellationToken); throw; }
    }
}
