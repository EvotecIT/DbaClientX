using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using DBAClientX.Diagnostics;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

public abstract partial class DatabaseClientBase
{
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Asynchronously executes a query and streams rows using <see cref="IAsyncEnumerable{T}"/>.
    /// </summary>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the iteration.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <param name="dbParameters">Provider-specific parameters to attach directly to the command.</param>
    /// <param name="commandType">Command type to use (Text or StoredProcedure).</param>
    /// <returns>An asynchronous stream of <see cref="DataRow"/> instances.</returns>
    protected virtual IAsyncEnumerable<DataRow> ExecuteQueryStreamAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        IEnumerable<DbParameter>? dbParameters = null,
        CommandType commandType = CommandType.Text)
        => DbaClientXDiagnostics.ObserveStream(ExecuteQueryStreamCoreAsync(connection, transaction, query, parameters,
            cancellationToken, parameterTypes, parameterDirections, dbParameters, commandType), connection, query, cancellationToken);

    private async IAsyncEnumerable<DataRow> ExecuteQueryStreamCoreAsync(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        IDictionary<string, object?>? parameters = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        IEnumerable<DbParameter>? dbParameters = null,
        CommandType commandType = CommandType.Text)
    {
        ValidateCommandText(query, commandType);
        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        command.CommandType = commandType;
        AddParameters(command, parameters, parameterTypes, parameterDirections);
        AddParameters(command, dbParameters);
        ApplyCommandTimeout(command);

        await using var reader = await ExecuteCommandWithRetryAsync(
            () => AwaitWithCallerCancellationAsync(
                () => command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken),
                cancellationToken),
            connection,
            transaction,
            cancellationToken).ConfigureAwait(false);

        var fieldCount = reader.FieldCount;
        var columnNames = new string[fieldCount];
        var columnTypes = new Type[fieldCount];
        var usedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < fieldCount; i++)
        {
            columnNames[i] = GetUniqueColumnName(reader.GetName(i), i, usedColumnNames);
            columnTypes[i] = reader.GetFieldType(i);
        }

        // Streamed rows are detached, so a column type cannot change once rows exist. When values require a different
        // type, later rows are created from a new table with the adapted schema; rows already yielded keep their table.
        var table = CreateStreamTable(columnNames, columnTypes);
        var observedValues = AdaptResultColumnTypesToValues ? new bool[fieldCount] : null;
        var values = new object[fieldCount];

        while (await AwaitWithCallerCancellationAsync(
            () => reader.ReadAsync(cancellationToken),
            cancellationToken).ConfigureAwait(false))
        {
            for (int i = 0; i < fieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? DBNull.Value : reader.GetValue(i);
            }

            if (observedValues != null && AdaptColumnTypesToValues(columnTypes, values, observedValues))
            {
                table = CreateStreamTable(columnNames, columnTypes);
            }

            table = AdaptStreamDateTimeModes(table, values);
            var row = table.NewRow();
            row.ItemArray = values;
            yield return row;
        }

        UpdateOutputParameters(command, parameters);
        yield break;
    }

    /// <summary>
    /// Asynchronously streams query rows through a caller-provided mapper.
    /// </summary>
    /// <typeparam name="T">The row result type produced by <paramref name="map"/>.</typeparam>
    /// <param name="connection">The open database connection.</param>
    /// <param name="transaction">The transaction to enlist in, if any.</param>
    /// <param name="query">The query to execute.</param>
    /// <param name="map">A mapper that converts the current data record into a result value.</param>
    /// <param name="initialize">Optional callback invoked once after the reader opens and before the first row is read.</param>
    /// <param name="parameters">Optional parameter values.</param>
    /// <param name="cancellationToken">Token used to cancel the iteration.</param>
    /// <param name="parameterTypes">Optional parameter types.</param>
    /// <param name="parameterDirections">Optional parameter directions.</param>
    /// <param name="dbParameters">Provider-specific parameters to attach directly to the command.</param>
    /// <param name="commandType">Command type to use (Text or StoredProcedure).</param>
    /// <returns>An asynchronous stream of mapped result rows.</returns>
    protected virtual IAsyncEnumerable<T> ExecuteMappedQueryStreamAsync<T>(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        Func<IDataRecord, T> map,
        Action<IDataRecord>? initialize = null,
        IDictionary<string, object?>? parameters = null,
        CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        IEnumerable<DbParameter>? dbParameters = null,
        CommandType commandType = CommandType.Text)
        => DbaClientXDiagnostics.ObserveStream(ExecuteMappedQueryStreamCoreAsync(connection, transaction, query, map,
            initialize, parameters, cancellationToken, parameterTypes, parameterDirections, dbParameters, commandType), connection, query, cancellationToken);

    private async IAsyncEnumerable<T> ExecuteMappedQueryStreamCoreAsync<T>(
        DbConnection connection,
        DbTransaction? transaction,
        string query,
        Func<IDataRecord, T> map,
        Action<IDataRecord>? initialize = null,
        IDictionary<string, object?>? parameters = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default,
        IDictionary<string, DbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        IEnumerable<DbParameter>? dbParameters = null,
        CommandType commandType = CommandType.Text)
    {
        ValidateCommandText(query, commandType);
        if (map == null)
        {
            throw new ArgumentNullException(nameof(map));
        }
        using var command = connection.CreateCommand();
        command.CommandText = query;
        command.Transaction = transaction;
        command.CommandType = commandType;
        AddParameters(command, parameters, parameterTypes, parameterDirections);
        AddParameters(command, dbParameters);
        ApplyCommandTimeout(command);

        await using var reader = await ExecuteCommandWithRetryAsync(
            () => AwaitWithCallerCancellationAsync(
                () => command.ExecuteReaderAsync(CommandBehavior.Default, cancellationToken),
                cancellationToken),
            connection,
            transaction,
            cancellationToken).ConfigureAwait(false);
        initialize?.Invoke(reader);
        while (await AwaitWithCallerCancellationAsync(
            () => reader.ReadAsync(cancellationToken),
            cancellationToken).ConfigureAwait(false))
        {
            yield return map(reader);
        }

        UpdateOutputParameters(command, parameters);
        yield break;
    }
#endif
}
