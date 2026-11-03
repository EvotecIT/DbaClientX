using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DBAClientX;

/// <summary>Reuses canonical parameter binding and materialization without connection ownership or retries.</summary>
internal sealed class SqlServerLockedCommandClient : DatabaseClientBase
{
    internal SqlServerLockedCommandClient()
    {
        MaxRetryAttempts = 1;
        RetryNonQueryOperations = false;
        // Complete every result set before a locked operation can be committed.
        // SqlClient may suppress a later batch error while disposing a reader.
        ReturnType = ReturnType.DataSet;
    }

    internal Task<int> NonQueryAsync(SqlConnection connection, SqlTransaction transaction, string query,
        IDictionary<string, object?>? parameters, CancellationToken token) =>
        ExecuteNonQueryAsync(connection, transaction, query, parameters, token);

    internal async Task<DataTable> TableAsync(SqlConnection connection, SqlTransaction transaction, string query,
        IDictionary<string, object?>? parameters, CancellationToken token)
    {
        var result = await ExecuteQueryAsync(connection, transaction, query, parameters, token)
            .ConfigureAwait(false) as DataSet
            ?? throw new InvalidOperationException("The locked SQL query did not produce a data set.");
        return result.Tables.Count > 0 ? result.Tables[0] : new DataTable("Table0");
    }
}
