using System;
using System.Threading;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using SQLitePCL;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>The SQLite result code of a statement stopped by <c>sqlite3_interrupt</c> (<c>SQLITE_INTERRUPT</c>).</summary>
    internal const int SqliteInterruptErrorCode = 9;

    /// <inheritdoc />
    protected override bool IsProviderCancellationException(Exception exception)
    {
        if (base.IsProviderCancellationException(exception))
        {
            return true;
        }

        return ExceptionChainContains<SqliteException>(
            exception,
            static sqliteException => sqliteException.SqliteErrorCode == SqliteInterruptErrorCode);
    }

    /// <summary>
    /// Interrupts the statement running on <paramref name="connection"/> when <paramref name="cancellationToken"/> is
    /// canceled.
    /// </summary>
    /// <remarks>
    /// Microsoft.Data.Sqlite steps statements synchronously: it checks the token only before a statement starts, and
    /// <see cref="SqliteCommand.Cancel"/> does nothing, so a canceled query otherwise runs to completion.
    /// <c>sqlite3_interrupt</c> is safe to call from another thread and stops the running statement with
    /// <c>SQLITE_INTERRUPT</c>, which the cancellation normalization reports as <see cref="OperationCanceledException"/>.
    /// An interrupt that arrives when no statement runs does not affect the next statement. Dispose the registration
    /// before the connection is closed or reused. Register only while an operation exclusively uses a connection outside a
    /// transaction: an interrupted write inside
    /// an explicit transaction rolls the whole transaction back.
    /// </remarks>
    /// <param name="connection">An open connection owned by the current operation.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <param name="commandText">Optional SQL text; batches that start a transaction retain cooperative cancellation.</param>
    /// <returns>The registration to dispose when the operation ends.</returns>
    internal static CancellationTokenRegistration RegisterStatementInterrupt(
        SqliteConnection connection,
        CancellationToken cancellationToken,
        string? commandText = null)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        return cancellationToken.CanBeCanceled && IsInAutoCommitMode(connection) && !StartsSqlTransaction(commandText)
            ? cancellationToken.Register(static state => InterruptStatement((SqliteConnection)state!), connection)
            : default;
    }

    /// <summary>Registers <see cref="RegisterStatementInterrupt"/> only when the operation owns the connection.</summary>
    /// <remarks>
    /// A shared transaction connection is never interrupted: an interrupted write would roll back the caller's whole
    /// transaction behind the transaction state this client keeps.
    /// </remarks>
    private static CancellationTokenRegistration RegisterOwnedStatementInterrupt(
        SqliteConnection connection,
        bool ownsConnection,
        CancellationToken cancellationToken,
        string commandText)
        => ownsConnection ? RegisterStatementInterrupt(connection, cancellationToken, commandText) : default;

    private static void InterruptStatement(SqliteConnection connection)
    {
        var handle = connection.Handle;
        if (handle != null)
        {
            // Never query transaction state here: sqlite3_get_autocommit can wait for the running
            // statement's mutex. Only sqlite3_interrupt is safe for cancellation from another thread.
            raw.sqlite3_interrupt(handle);
        }
    }

    // SQL BEGIN and SAVEPOINT do not create a managed SqliteTransaction. Query native state so both
    // cancellation and replay protect earlier writes regardless of how a transaction was started.
    private static bool IsInAutoCommitMode(SqliteConnection connection)
        => connection.Handle is { } handle && raw.sqlite3_get_autocommit(handle) != 0;

    /// <inheritdoc />
    protected override bool CanRetryCommand(DbConnection connection, DbTransaction? transaction, bool returnsResults)
        => base.CanRetryCommand(connection, transaction, returnsResults)
           && connection is SqliteConnection sqliteConnection && IsInAutoCommitMode(sqliteConnection);

    private static bool StartsSqlTransaction(string? commandText)
    {
        if (commandText == null ||
            (commandText.IndexOf("BEGIN", StringComparison.OrdinalIgnoreCase) < 0 &&
             commandText.IndexOf("SAVEPOINT", StringComparison.OrdinalIgnoreCase) < 0)) return false;

        foreach (string statement in QueryPlans.SqlStatementText.Split(commandText))
            if (StartsWithKeyword(statement, "BEGIN") || StartsWithKeyword(statement, "SAVEPOINT")) return true;
        return false;
    }

    private static bool StartsWithKeyword(string statement, string keyword)
        => statement.StartsWith(keyword, StringComparison.OrdinalIgnoreCase) &&
           (statement.Length == keyword.Length ||
            !(char.IsLetterOrDigit(statement[keyword.Length]) || statement[keyword.Length] is '_' or '$'));
}
