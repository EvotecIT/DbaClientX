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

    // Reuse the session execution policy without recreating the prepared statement.
    internal int ExecutePreparedNonQuery(SqliteCommand command)
        => RetryNonQueryOperations ? ExecuteWithRetry(command.ExecuteNonQuery) : command.ExecuteNonQuery();

    internal object? ExecutePreparedScalar(SqliteCommand command)
        => ExecuteWithRetry(command.ExecuteScalar);

    internal Task<int> ExecutePreparedNonQueryAsync(SqliteCommand command, CancellationToken cancellationToken)
    {
        Task<int> ExecuteAsync() => AwaitWithCallerCancellationAsync(
            () => command.ExecuteNonQueryAsync(cancellationToken), cancellationToken);
        return RetryNonQueryOperations
            ? ExecuteWithRetryAsync(ExecuteAsync, cancellationToken)
            : ExecuteAsync();
    }

    internal Task<object?> ExecutePreparedScalarAsync(SqliteCommand command, CancellationToken cancellationToken)
        => ExecuteWithRetryAsync(() => AwaitWithCallerCancellationAsync(
            () => command.ExecuteScalarAsync(cancellationToken), cancellationToken), cancellationToken);
}
