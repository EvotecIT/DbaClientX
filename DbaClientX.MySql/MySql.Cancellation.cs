using System;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
    /// <inheritdoc />
    protected override bool IsProviderCancellationException(Exception exception)
    {
        if (base.IsProviderCancellationException(exception))
        {
            return true;
        }

        return ExceptionChainContains<MySqlException>(
            exception,
            static mySqlException => mySqlException.ErrorCode == MySqlErrorCode.QueryInterrupted);
    }
}
