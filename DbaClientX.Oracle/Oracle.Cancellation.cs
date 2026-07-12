using System;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    private const int UserRequestedCancellationErrorNumber = 1013;

    /// <inheritdoc />
    protected override bool IsProviderCancellationException(Exception exception)
    {
        if (base.IsProviderCancellationException(exception))
        {
            return true;
        }

        return ExceptionChainContains<OracleException>(
            exception,
            static oracleException => oracleException.Number == UserRequestedCancellationErrorNumber);
    }
}
