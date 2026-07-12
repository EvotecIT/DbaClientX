using System;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

public partial class SqlServer
{
    /// <inheritdoc />
    protected override bool IsProviderCancellationException(Exception exception)
    {
        if (base.IsProviderCancellationException(exception))
        {
            return true;
        }

        return ExceptionChainContains<SqlException>(exception, static sqlException =>
        {
            if (sqlException.Number != 0 ||
                sqlException.Class != 11 ||
                sqlException.State != 0 ||
                sqlException.Errors.Count == 0)
            {
                return false;
            }

            var hasOnlyCancellationErrors = true;
            foreach (SqlError error in sqlException.Errors)
            {
                if (error.Number != 0 || error.Class != 11 || error.State != 0)
                {
                    hasOnlyCancellationErrors = false;
                    break;
                }
            }

            return hasOnlyCancellationErrors;
        });
    }
}
