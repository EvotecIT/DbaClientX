using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using DBAClientX.Diagnostics;

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

        // The connection factory snapshots DefaultTimeout into the command. The native constructor
        // leaves that override unset, so prepared statements follow retained timeout changes and resets.
        var command = new SqliteCommand(query, connection, transaction);
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
        => ExecuteCommandWithDiagnostics(command.ExecuteNonQuery, command.Connection!, command.Transaction,
            command.CommandText, "prepared.nonquery", static rows => rows < 0 ? null : rows, returnsResults: false);

    internal object? ExecutePreparedScalar(SqliteCommand command)
        => ExecuteCommandWithDiagnostics(command.ExecuteScalar, command.Connection!, command.Transaction,
            command.CommandText, "prepared.scalar");

    internal async Task<int> ExecutePreparedNonQueryAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        using var interrupt = command.Transaction == null
            ? RegisterCommandInterrupt(command.Connection!, cancellationToken, command.CommandText) : default;
        return await ExecutePreparedCommandAsync(command,
            static (statement, token) => statement.ExecuteNonQueryAsync(token),
            cancellationToken, returnsResults: false).ConfigureAwait(false);
    }

    internal async Task<object?> ExecutePreparedScalarAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        using var interrupt = command.Transaction == null
            ? RegisterCommandInterrupt(command.Connection!, cancellationToken, command.CommandText) : default;
        return await ExecutePreparedCommandAsync(command,
            static (statement, token) => statement.ExecuteScalarAsync(token),
            cancellationToken, returnsResults: true).ConfigureAwait(false);
    }

    private Task<T> ExecutePreparedCommandAsync<T>(SqliteCommand command,
        Func<SqliteCommand, CancellationToken, Task<T>> operation, CancellationToken cancellationToken, bool returnsResults)
        => DbaClientXDiagnostics.IsCommandObserved
            ? ExecuteObservedPreparedCommandAsync(command, operation, cancellationToken, returnsResults)
            : ExecutePreparedCommandCoreAsync(command, operation, cancellationToken, returnsResults);

    private async Task<T> ExecuteObservedPreparedCommandAsync<T>(SqliteCommand command,
        Func<SqliteCommand, CancellationToken, Task<T>> operation, CancellationToken token, bool returnsResults)
    {
        using var scope = DbaClientXDiagnostics.StartCommand(command.Connection!, command.CommandText,
            returnsResults ? "prepared.scalar" : "prepared.nonquery");
        try
        {
            var result = await ExecutePreparedCommandCoreAsync(command, operation, token, returnsResults).ConfigureAwait(false);
            scope.Complete(!returnsResults && result is int rows && rows >= 0 ? rows : null);
            return result;
        }
        catch (Exception exception) { scope.Fail(exception, token); throw; }
    }

    private Task<T> ExecutePreparedCommandCoreAsync<T>(SqliteCommand command,
        Func<SqliteCommand, CancellationToken, Task<T>> operation, CancellationToken cancellationToken, bool returnsResults)
    {
        if (cancellationToken.IsCancellationRequested) return Task.FromCanceled<T>(cancellationToken);
        return CanRetryCommand(command.Connection!, command.Transaction, returnsResults)
            ? ExecutePreparedReplayableCommandAsync(command, operation, cancellationToken, returnsResults)
            : AwaitWithCallerCancellationAsync(operation, command, cancellationToken);
    }

    private Task<T> ExecutePreparedReplayableCommandAsync<T>(SqliteCommand command,
        Func<SqliteCommand, CancellationToken, Task<T>> operation, CancellationToken cancellationToken, bool returnsResults)
        => ExecuteCommandWithRetryAsync(() => AwaitWithCallerCancellationAsync(operation, command, cancellationToken),
            command.Connection!, command.Transaction, cancellationToken, returnsResults);
}
