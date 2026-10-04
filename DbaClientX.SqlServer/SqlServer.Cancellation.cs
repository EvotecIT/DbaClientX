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
            if (sqlException.Errors.Count == 0)
            {
                return false;
            }

            bool hasCancellation = false;
            foreach (SqlError error in sqlException.Errors)
            {
                if (error.Number == 0 && error.Class == 11 && error.State == 0)
                {
                    hasCancellation = true;
                    continue;
                }
                // A native BACKUP/RESTORE attention can prepend "aborted" (3204) and
                // "terminating abnormally" (3013). Neither establishes cancellation alone.
                // Any other failure must remain an error even if the caller also cancelled.
                if (error.Class == 16 && (error.Number == 3204 || error.Number == 3013))
                {
                    continue;
                }
                return false;
            }

            return hasCancellation;
        });
    }
}
