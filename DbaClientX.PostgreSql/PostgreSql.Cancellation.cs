using System;
using Npgsql;

namespace DBAClientX;

public partial class PostgreSql
{
    /// <inheritdoc />
    protected override bool IsProviderCancellationException(Exception exception)
    {
        if (base.IsProviderCancellationException(exception))
        {
            return true;
        }

        return ExceptionChainContains<PostgresException>(
            exception,
            static postgresException =>
                string.Equals(
                    postgresException.SqlState,
                    PostgresErrorCodes.QueryCanceled,
                    StringComparison.Ordinal) &&
                (postgresException.MessageText.IndexOf("user request", StringComparison.OrdinalIgnoreCase) >= 0 ||
                 (postgresException.Detail?.IndexOf("user request", StringComparison.OrdinalIgnoreCase) ?? -1) >= 0));
    }
}
