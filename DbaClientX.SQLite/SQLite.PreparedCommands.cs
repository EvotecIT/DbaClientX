using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

public partial class SQLite
{
    /// <summary>
    /// Creates a command bound to a session's connection and transaction and prepares it once, so callers can
    /// execute it many times while only rebinding parameter values.
    /// </summary>
    internal SQLitePreparedCommand CreateSessionPreparedCommand(
        SqliteConnection connection,
        SqliteTransaction? transaction,
        string query,
        IReadOnlyList<string> parameterNames)
    {
        ValidateCommandText(query);
        if (parameterNames == null)
        {
            throw new ArgumentNullException(nameof(parameterNames));
        }

        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = query;
        ApplyCommandTimeout(command);
        var parameters = new SqliteParameter[parameterNames.Count];
        for (var index = 0; index < parameterNames.Count; index++)
        {
            string name = parameterNames[index];
            if (string.IsNullOrWhiteSpace(name))
            {
                command.Dispose();
                throw new ArgumentException("Parameter names cannot be null or whitespace.", nameof(parameterNames));
            }

            // Untyped parameters bind each value by its runtime type, so integers stay integers.
            var parameter = command.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = DBNull.Value;
            command.Parameters.Add(parameter);
            parameters[index] = parameter;
        }

        try
        {
            command.Prepare();
        }
        catch (SqliteException ex)
        {
            command.Dispose();
            throw CreateQueryExecutionException("Failed to prepare command.", query, ex);
        }

        return new SQLitePreparedCommand(this, command, parameters);
    }

    /// <summary>
    /// Builds <c>INSERT INTO "table" ("a", "b") VALUES ($p0, $p1)</c> with quoted identifiers and positional
    /// parameter names, so column names never need to be valid parameter names.
    /// </summary>
    internal static (string Query, string[] ParameterNames) BuildPreparedInsert(string table, IReadOnlyList<string> columns)
    {
        if (columns == null)
        {
            throw new ArgumentNullException(nameof(columns));
        }

        if (columns.Count == 0)
        {
            throw new ArgumentException("At least one column is required.", nameof(columns));
        }

        var names = new string[columns.Count];
        var query = new StringBuilder("INSERT INTO ").Append(QuoteIdentifierPath(table)).Append(" (");
        for (var index = 0; index < columns.Count; index++)
        {
            query.Append(index == 0 ? string.Empty : ", ").Append(QuoteIdentifier(columns[index]));
            names[index] = "$p" + index.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        query.Append(") VALUES (").Append(string.Join(", ", names)).Append(");");
        return (query.ToString(), names);
    }

    internal DbaQueryExecutionException CreatePreparedCommandException(string message, string query, Exception exception)
        => CreateQueryExecutionException(message, query, exception);

    // Apply the same replay and cancellation policy as ordinary/session execution.
    internal int ExecutePreparedNonQuery(SqliteCommand command)
        => ExecuteCommandWithRetry(command.ExecuteNonQuery, command.Connection!, command.Transaction, returnsResults: false);

    internal object? ExecutePreparedScalar(SqliteCommand command)
        => ExecuteCommandWithRetry(command.ExecuteScalar, command.Connection!, command.Transaction);

    internal async Task<int> ExecutePreparedNonQueryAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        using var interrupt = command.Transaction == null
            ? RegisterCommandInterrupt(command.Connection!, cancellationToken, command.CommandText) : default;
        return await ExecuteCommandWithRetryAsync(
            () => AwaitWithCallerCancellationAsync(
                () => command.ExecuteNonQueryAsync(cancellationToken), cancellationToken),
            command.Connection!,
            command.Transaction, cancellationToken, returnsResults: false).ConfigureAwait(false);
    }

    internal async Task<object?> ExecutePreparedScalarAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        using var interrupt = command.Transaction == null
            ? RegisterCommandInterrupt(command.Connection!, cancellationToken, command.CommandText) : default;
        return await ExecuteCommandWithRetryAsync(
            () => AwaitWithCallerCancellationAsync(
                () => command.ExecuteScalarAsync(cancellationToken), cancellationToken),
            command.Connection!,
            command.Transaction, cancellationToken).ConfigureAwait(false);
    }
}
