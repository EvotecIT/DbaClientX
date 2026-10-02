using System;
using System.Threading;
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
    /// before the connection is closed. Register only on connections the operation owns: an interrupted write inside
    /// an explicit transaction rolls the whole transaction back.
    /// </remarks>
    /// <param name="connection">An open connection owned by the current operation.</param>
    /// <param name="cancellationToken">The caller's token.</param>
    /// <returns>The registration to dispose when the operation ends.</returns>
    internal static CancellationTokenRegistration RegisterStatementInterrupt(
        SqliteConnection connection,
        CancellationToken cancellationToken)
    {
        if (connection == null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        return cancellationToken.CanBeCanceled
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
        CancellationToken cancellationToken)
        => ownsConnection ? RegisterStatementInterrupt(connection, cancellationToken) : default;

    private static void InterruptStatement(SqliteConnection connection)
    {
        var handle = connection.Handle;
        if (handle != null)
        {
            raw.sqlite3_interrupt(handle);
        }
    }
}
