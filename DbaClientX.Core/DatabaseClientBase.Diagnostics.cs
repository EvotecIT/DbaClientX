using System.Data;
using System.Data.Common;
using DBAClientX.Diagnostics;

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
    internal T ExecuteCommandWithDiagnostics<T>(Func<T> operation, DbConnection connection, DbTransaction? transaction,
        string query, string kind, Func<T, long?>? countRows = null, bool returnsResults = true)
    {
        if (!DbaClientXDiagnostics.IsCommandObserved)
            return ExecuteCommandWithRetry(operation, connection, transaction, returnsResults);
        using var scope = DbaClientXDiagnostics.StartCommand(connection, query, kind);
        try
        {
            var result = ExecuteCommandWithRetry(operation, connection, transaction, returnsResults);
            scope.Complete(countRows?.Invoke(result));
            return result;
        }
        catch (Exception exception) { scope.Fail(exception); throw; }
    }

    internal Task<T> ExecuteCommandWithDiagnosticsAsync<T>(Func<Task<T>> operation, DbConnection connection, DbTransaction? transaction,
        string query, string kind, CancellationToken token, Func<T, long?>? countRows = null, bool returnsResults = true)
        => !DbaClientXDiagnostics.IsCommandObserved
            ? ExecuteCommandWithRetryAsync(operation, connection, transaction, token, returnsResults)
            : ExecuteObservedCommandAsync(operation, connection, transaction, query, kind, token, countRows, returnsResults);

    private async Task<T> ExecuteObservedCommandAsync<T>(Func<Task<T>> operation, DbConnection connection, DbTransaction? transaction,
        string query, string kind, CancellationToken token, Func<T, long?>? countRows, bool returnsResults)
    {
        using var scope = DbaClientXDiagnostics.StartCommand(connection, query, kind);
        try
        {
            var result = await ExecuteCommandWithRetryAsync(operation, connection, transaction, token, returnsResults).ConfigureAwait(false);
            scope.Complete(countRows?.Invoke(result));
            return result;
        }
        catch (Exception exception) { scope.Fail(exception, token); throw; }
    }

    private static long? MaterializedRowCount(object? result)
    {
        if (result is DataRow) return 1;
        if (result is DataTable table) return table.Rows.Count;
        if (result is DataSet dataSet)
        {
            long rows = 0;
            foreach (DataTable item in dataSet.Tables) rows += item.Rows.Count;
            return rows;
        }
        return result == null ? 0 : null;
    }

    /// <summary>Executes a reader-open operation under command replay policy and optional startup diagnostics.</summary>
    /// <typeparam name="T">The provider reader type.</typeparam>
    /// <param name="operation">One complete reader-open attempt.</param>
    /// <param name="connection">The open native connection.</param>
    /// <param name="transaction">The transaction, if any.</param>
    /// <param name="query">The statement used only to compute an observed fingerprint.</param>
    /// <returns>The reader. Ownership and row consumption remain with its caller.</returns>
    /// <remarks>The duration ends at reader handoff and does not include later row consumption.</remarks>
    protected T ExecuteReaderWithDiagnostics<T>(Func<T> operation, DbConnection connection, DbTransaction? transaction, string query)
        => ExecuteCommandWithDiagnostics(operation, connection, transaction, query, "reader.open");

    /// <summary>Asynchronously executes a reader-open operation under replay policy and optional startup diagnostics.</summary>
    /// <typeparam name="T">The provider reader type.</typeparam>
    /// <param name="operation">One complete reader-open attempt.</param>
    /// <param name="connection">The open native connection.</param>
    /// <param name="transaction">The transaction, if any.</param>
    /// <param name="query">The statement used only to compute an observed fingerprint.</param>
    /// <param name="cancellationToken">The reader-open cancellation token.</param>
    /// <returns>The reader. Ownership and row consumption remain with its caller.</returns>
    /// <remarks>The duration ends at reader handoff and does not include later row consumption.</remarks>
    protected Task<T> ExecuteReaderWithDiagnosticsAsync<T>(Func<Task<T>> operation, DbConnection connection,
        DbTransaction? transaction, string query, CancellationToken cancellationToken)
        => ExecuteCommandWithDiagnosticsAsync(operation, connection, transaction, query, "reader.open", cancellationToken);

    /// <summary>Opens a native connection while observing its provider-open duration when diagnostics are enabled.</summary>
    /// <param name="connection">The connection to open.</param>
    protected void OpenConnectionWithDiagnostics(DbConnection connection)
        => DbaClientXDiagnostics.OpenConnection(connection);

    /// <summary>Opens a native connection asynchronously while preserving its task when diagnostics are disabled.</summary>
    /// <param name="connection">The connection to open.</param>
    /// <param name="cancellationToken">The provider-open cancellation token.</param>
    /// <returns>The native open task, or its observed completion.</returns>
    protected Task OpenConnectionWithDiagnosticsAsync(DbConnection connection, CancellationToken cancellationToken)
        => DbaClientXDiagnostics.OpenConnectionAsync(connection, cancellationToken);
}
