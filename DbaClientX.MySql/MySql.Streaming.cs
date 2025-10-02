using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DBAClientX;

public partial class MySql
{
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Streams rows produced by a query asynchronously without buffering the entire result set.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            MySqlConnection? connection = null;
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
                (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection!, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    connection?.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Streams rows produced by a stored procedure asynchronously without buffering the entire result set.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
        string host,
        string database,
        string username,
        string password,
        string procedure,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            MySqlConnection? connection = null;
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
                (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection!, useTransaction ? _transaction : null, procedure, parameters, cancellationToken, dbTypes, parameterDirections, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    connection?.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Streams rows produced by a stored procedure using explicitly constructed <see cref="DbParameter"/> instances.
    /// </summary>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(
        string host,
        string database,
        string username,
        string password,
        string procedure,
        IEnumerable<DbParameter>? parameters = null,
        bool useTransaction = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            MySqlConnection? connection = null;
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
                (connection, dispose) = await ResolveConnectionAsync(connectionString, useTransaction, cancellationToken).ConfigureAwait(false);
            }

            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection!, useTransaction ? _transaction : null, procedure, cancellationToken: cancellationToken, dbParameters: parameters, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
                {
                    yield return row;
                }
            }
            finally
            {
                if (dispose)
                {
                    connection?.Dispose();
                }
            }
        }
    }
#endif
}
