using System;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    private const int SqliteInterruptErrorCode = 9;

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
}
