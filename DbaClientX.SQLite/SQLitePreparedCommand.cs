using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace DBAClientX;

/// <summary>
/// A SQLite statement prepared once on a session's connection (and transaction, when created inside one) that can
/// be executed many times with new parameter values. This is the fast path for writing many rows: SQLite parses and
/// plans the statement once, and each execution only rebinds values.
/// </summary>
/// <remarks>
/// Create it from <see cref="SQLiteSession.Prepare(string, string[])"/>, <see cref="SQLiteSession.PrepareInsert(string, string[])"/>
/// or their <see cref="SQLiteAsyncSession"/> equivalents. Values are bound by position in the order the parameter names
/// were given; <see langword="null"/> is stored as NULL and enums as their underlying integer. Dispose the command before
/// the session (or its transaction) ends.
/// </remarks>
public sealed class SQLitePreparedCommand : IDisposable
{
    private readonly SQLite _client;
    private readonly SqliteCommand _command;
    private readonly SqliteParameter[] _parameters;
    private bool _disposed;

    internal SQLitePreparedCommand(SQLite client, SqliteCommand command, SqliteParameter[] parameters)
    {
        _client = client;
        _command = command;
        _parameters = parameters;
    }

    /// <summary>
    /// Number of values each execution expects.
    /// </summary>
    public int ParameterCount => _parameters.Length;

    /// <summary>
    /// Executes the statement with the given values, in parameter order.
    /// </summary>
    /// <param name="values">One value per parameter.</param>
    /// <returns>The number of rows affected.</returns>
    public int ExecuteNonQuery(params object?[] values)
    {
        Bind(values);
        try
        {
            return _client.ExecutePreparedNonQuery(_command);
        }
        catch (SqliteException ex)
        {
            throw _client.CreatePreparedCommandException("Failed to execute prepared command.", _command.CommandText, ex);
        }
    }

    /// <summary>
    /// Executes the statement with the given values and returns the first column of the first row.
    /// </summary>
    /// <param name="values">One value per parameter.</param>
    /// <returns>The scalar value, or <see langword="null"/> when the statement returned no rows.</returns>
    public object? ExecuteScalar(params object?[] values)
    {
        Bind(values);
        try
        {
            return Normalize(_client.ExecutePreparedScalar(_command));
        }
        catch (SqliteException ex)
        {
            throw _client.CreatePreparedCommandException("Failed to execute prepared command.", _command.CommandText, ex);
        }
    }

    /// <summary>
    /// Asynchronously executes the statement with the given values, in parameter order.
    /// </summary>
    /// <param name="values">One value per parameter.</param>
    /// <param name="cancellationToken">Token used to cancel execution.</param>
    /// <returns>The number of rows affected.</returns>
    public async Task<int> ExecuteNonQueryAsync(IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        Bind(values);
        try
        {
            return await _client.ExecutePreparedNonQueryAsync(_command, cancellationToken).ConfigureAwait(false);
        }
        catch (SqliteException ex)
        {
            throw _client.CreatePreparedCommandException("Failed to execute prepared command.", _command.CommandText, ex);
        }
    }

    /// <summary>
    /// Asynchronously executes the statement with the given values and returns the first column of the first row.
    /// </summary>
    /// <param name="values">One value per parameter.</param>
    /// <param name="cancellationToken">Token used to cancel execution.</param>
    /// <returns>The scalar value, or <see langword="null"/> when the statement returned no rows.</returns>
    public async Task<object?> ExecuteScalarAsync(IReadOnlyList<object?> values, CancellationToken cancellationToken = default)
    {
        Bind(values);
        try
        {
            return Normalize(await _client.ExecutePreparedScalarAsync(_command, cancellationToken).ConfigureAwait(false));
        }
        catch (SqliteException ex)
        {
            throw _client.CreatePreparedCommandException("Failed to execute prepared command.", _command.CommandText, ex);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _command.Dispose();
        _disposed = true;
    }

    private void Bind(IReadOnlyList<object?> values)
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(SQLitePreparedCommand));
        }

        if (values == null)
        {
            throw new ArgumentNullException(nameof(values));
        }

        if (values.Count != _parameters.Length)
        {
            throw new ArgumentException(
                $"Expected {_parameters.Length} value(s) for the prepared command but received {values.Count}.",
                nameof(values));
        }

        for (var index = 0; index < _parameters.Length; index++)
        {
            object? value = values[index];
            _parameters[index].Value = value switch
            {
                null => DBNull.Value,
                Enum enumValue => Convert.ToInt64(enumValue, System.Globalization.CultureInfo.InvariantCulture),
                _ => value
            };
        }
    }

    private static object? Normalize(object? value) => value is DBNull ? null : value;
}
