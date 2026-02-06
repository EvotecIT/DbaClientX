#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Streams query results asynchronously, yielding one <see cref="DataRow"/> at a time.
    /// </summary>
    public virtual async IAsyncEnumerable<DataRow> QueryStreamAsync(
        string database,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, SqliteType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildOperationalConnectionString(database);

        SqliteConnection? connection = null;
        var dispose = false;

        if (useTransaction)
        {
            if (_transaction == null || _transactionConnection == null)
            {
                throw new DbaTransactionException("Transaction has not been started.");
            }

            connection = _transactionConnection;
        }
        else
        {
            (connection, dispose) = await ResolveConnectionAsync(connectionString, false, cancellationToken).ConfigureAwait(false);
        }

        var dbTypes = ConvertParameterTypes(parameterTypes);
        if (connection == null)
        {
            throw new DbaQueryExecutionException("Failed to resolve connection for streaming.", query, new InvalidOperationException("The SQLite connection could not be resolved."));
        }

        try
        {
            await foreach (var row in base.ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
            {
                yield return row;
            }
        }
        finally
        {
            if (dispose)
            {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
                await connection.DisposeAsync().ConfigureAwait(false);
#else
                connection.Dispose();
#endif
            }
        }
    }
}
#endif
