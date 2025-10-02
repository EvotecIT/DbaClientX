using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using Npgsql;
using NpgsqlTypes;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with a PostgreSQL database using the shared <see cref="DatabaseClientBase"/> abstractions.
/// </summary>
public class PostgreSql : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private NpgsqlConnection? _transactionConnection;
    private NpgsqlTransaction? _transaction;

    /// <summary>
    /// Gets a value indicating whether the client currently has an active transaction scope.
    /// </summary>
    /// <remarks>
    /// The flag is toggled when <see cref="BeginTransaction(string, string, string, string)"/> or
    /// <see cref="BeginTransactionAsync(string, string, string, string, System.Threading.CancellationToken)"/> is invoked and
    /// returns to <see langword="false"/> after <see cref="Commit"/>, <see cref="Rollback"/>, or the async counterparts dispose the transaction.
    /// </remarks>
    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Builds a <see cref="NpgsqlConnectionStringBuilder"/> connection string from individual connection components.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="port">Optional TCP port; when omitted the provider default is used.</param>
    /// <param name="ssl">Optional SSL requirement flag; <see langword="true"/> enforces TLS.</param>
    /// <returns>The generated connection string.</returns>
    /// <remarks>
    /// The builder enables connection pooling by default so repeated operations reuse the same socket where possible, lowering
    /// latency and resource consumption for high-frequency workloads. Adjust pooling-related properties on the returned string when connection storm scenarios require tighter control.
    /// </remarks>
    public static string BuildConnectionString(string host, string database, string username, string password, int? port = null, bool? ssl = null)
    {
        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = host,
            Database = database,
            Username = username,
            Password = password,
            Pooling = true
        };
        if (port.HasValue)
        {
            builder.Port = port.Value;
        }
        if (ssl.HasValue)
        {
            builder.SslMode = ssl.Value ? SslMode.Require : SslMode.Disable;
        }
        return builder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified PostgreSQL instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Exceptions are swallowed to keep the call lightweight; call
    /// <see cref="ExecuteScalar(string, string, string, string, string, IDictionary{string, object?}?, bool, IDictionary{string, NpgsqlDbType}?, IDictionary{string, ParameterDirection}?)" /> for detailed error information.
    /// </remarks>
    public virtual bool Ping(string host, string database, string username, string password)
    {
        try
        {
            ExecuteScalar(host, database, username, password, "SELECT 1");
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Performs an asynchronous connectivity test against the specified PostgreSQL instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying query.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// The method mirrors <see cref="Ping"/> but uses async I/O primitives to avoid blocking threads.
    /// </remarks>
    public virtual async Task<bool> PingAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
    {
        try
        {
            await ExecuteScalarAsync(host, database, username, password, "SELECT 1", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The materialized query result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <remarks>
    /// Always prefer parameterized SQL by supplying <paramref name="parameters"/> (and optionally <paramref name="parameterTypes"/>) to guard against SQL injection attacks.
    /// Avoid concatenating user input into <paramref name="query"/> directly.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Executes a scalar SQL command and returns the first column of the first row in the result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The first column of the first row in the result set.</returns>
    /// <remarks>
    /// Use this helper for existence checks or when retrieving single aggregated values (such as <c>COUNT(*)</c>) without having to manually manage <see cref="NpgsqlCommand"/> instances.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteScalar(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteScalar(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, NpgsqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new NpgsqlParameter(), static (p, t) => p.NpgsqlDbType = t);

    /// <summary>
    /// Executes a SQL command that does not produce a result set (such as <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The number of affected rows.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual int ExecuteNonQuery(
        string host,
        string database,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, NpgsqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a SQL command that does not produce a result set (such as <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>A task containing the number of affected rows.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<int> ExecuteNonQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await base.ExecuteNonQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>A task returning the materialized query result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a scalar SQL command and returns the first column of the first row in the result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>A task returning the scalar result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteScalarAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            return await ExecuteScalarAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Executes a stored procedure and materializes the results into the shared <see cref="DatabaseClientBase"/> format.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The materialized stored procedure result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var dataSet = new DataSet();
            using var reader = command.ExecuteReader();
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && reader.NextResult());

            var result = BuildResult(dataSet);
            UpdateOutputParameters(command, parameters);
            return result;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a stored procedure and materializes the results into the shared <see cref="DatabaseClientBase"/> format.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>A task returning the materialized stored procedure result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            var dbTypes = ConvertParameterTypes(parameterTypes);
            AddParameters(command, parameters, dbTypes, parameterDirections);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

            var result = BuildResult(dataSet);
            UpdateOutputParameters(command, parameters);
            return result;
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Executes a stored procedure using pre-constructed database parameters.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional collection of fully-configured database parameters.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <returns>The materialized stored procedure result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                connection.Open();
                dispose = true;
            }

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            AddParameters(command, parameters);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var dataSet = new DataSet();
            using var reader = command.ExecuteReader();
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && reader.NextResult());

            return BuildResult(dataSet);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously executes a stored procedure using pre-constructed database parameters.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional collection of fully-configured database parameters.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <returns>A task returning the materialized stored procedure result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
        try
        {
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            using var command = connection.CreateCommand();
            command.CommandText = procedure;
            command.CommandType = CommandType.StoredProcedure;
            command.Transaction = useTransaction ? _transaction : null;
            AddParameters(command, parameters);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do
            {
                var table = new DataTable($"Table{tableIndex}");
                table.Load(reader);
                dataSet.Tables.Add(table);
                tableIndex++;
            } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

            return BuildResult(dataSet);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute stored procedure.", procedure, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
    /// <summary>
    /// Streams query results asynchronously, yielding rows as they become available.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>An async sequence of <see cref="DataRow"/> items.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            NpgsqlConnection? connection = null;
            bool dispose = false;
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes, parameterDirections).ConfigureAwait(false))
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
    /// Streams stored procedure results asynchronously, yielding rows as they become available.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="NpgsqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>An async sequence of <see cref="DataRow"/> items.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, NpgsqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            NpgsqlConnection? connection = null;
            bool dispose = false;
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            var dbTypes = ConvertParameterTypes(parameterTypes);
            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, procedure, parameters, cancellationToken, dbTypes, parameterDirections, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
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
    /// Streams stored procedure results asynchronously using pre-constructed database parameters.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional collection of fully-configured database parameters.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <returns>An async sequence of <see cref="DataRow"/> items.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            NpgsqlConnection? connection = null;
            bool dispose = false;
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
                connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                dispose = true;
            }

            try
            {
                await foreach (var row in ExecuteQueryStreamAsync(connection, useTransaction ? _transaction : null, procedure, cancellationToken: cancellationToken, dbParameters: parameters, commandType: CommandType.StoredProcedure).ConfigureAwait(false))
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

    /// <summary>
    /// Performs a bulk insert into a PostgreSQL destination table using the <c>COPY</c> protocol where possible.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="table">Source data represented as a <see cref="DataTable"/>.</param>
    /// <param name="destinationTable">Destination table name.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="batchSize">Optional batch size to split the input table.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (in seconds) applied to the bulk copy operation.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying bulk insert fails.</exception>
    public virtual void BulkInsert(string host, string database, string username, string password, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
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
            connection = CreateConnection(connectionString);
            OpenConnection(connection);
            dispose = true;
        }
        try
        {
            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (int offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    var batchTable = table.Clone();
                    for (int i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }
                    WriteTable(connection, batchTable, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null);
                }
            }
            else
            {
                WriteTable(connection, table, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null);
            }
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    /// <summary>
    /// Asynchronously performs a bulk insert into a PostgreSQL destination table using the <c>COPY</c> protocol where possible.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="table">Source data represented as a <see cref="DataTable"/>.</param>
    /// <param name="destinationTable">Destination table name.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="batchSize">Optional batch size to split the input table.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (in seconds) applied to the bulk copy operation.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying bulk insert fails.</exception>
    public virtual async Task BulkInsertAsync(string host, string database, string username, string password, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null, CancellationToken cancellationToken = default)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, database, username, password);

        NpgsqlConnection? connection = null;
        bool dispose = false;
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
            connection = CreateConnection(connectionString);
            await OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);
            dispose = true;
        }
        try
        {
            if (batchSize.HasValue && batchSize.Value > 0)
            {
                var totalRows = table.Rows.Count;
                for (int offset = 0; offset < totalRows; offset += batchSize.Value)
                {
                    var batchTable = table.Clone();
                    for (int i = offset; i < Math.Min(offset + batchSize.Value, totalRows); i++)
                    {
                        batchTable.ImportRow(table.Rows[i]);
                    }
                    await WriteTableAsync(connection, batchTable, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                await WriteTableAsync(connection, table, destinationTable, bulkCopyTimeout, useTransaction ? _transaction : null, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute bulk insert.", destinationTable, ex);
        }
        finally
        {
            if (dispose)
            {
                connection?.Dispose();
            }
        }
    }

    protected virtual void WriteTable(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction)
    {
        var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
        var copyCommand = $"COPY {destinationTable} ({columns}) FROM STDIN (FORMAT BINARY)";
        using var importer = connection.BeginBinaryImport(copyCommand);
        if (bulkCopyTimeout.HasValue)
        {
            importer.Timeout = TimeSpan.FromSeconds(bulkCopyTimeout.Value);
        }
        foreach (DataRow row in table.Rows)
        {
            importer.StartRow();
            foreach (DataColumn column in table.Columns)
            {
                var value = row[column];
                if (value == null || value == DBNull.Value)
                {
                    importer.WriteNull();
                }
                else
                {
                    importer.Write((dynamic)value);
                }
            }
        }
        importer.Complete();
    }

    protected virtual async Task WriteTableAsync(NpgsqlConnection connection, DataTable table, string destinationTable, int? bulkCopyTimeout, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var columns = string.Join(", ", table.Columns.Cast<DataColumn>().Select(c => $"\"{c.ColumnName}\""));
        var copyCommand = $"COPY {destinationTable} ({columns}) FROM STDIN (FORMAT BINARY)";
        await using var importer = await connection.BeginBinaryImportAsync(copyCommand, cancellationToken).ConfigureAwait(false);
        if (bulkCopyTimeout.HasValue)
        {
            importer.Timeout = TimeSpan.FromSeconds(bulkCopyTimeout.Value);
        }
        foreach (DataRow row in table.Rows)
        {
            await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);
            foreach (DataColumn column in table.Columns)
            {
                var value = row[column];
                if (value == null || value == DBNull.Value)
                {
                    await importer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await importer.WriteAsync((dynamic)value, cancellationToken: cancellationToken).ConfigureAwait(false);
                }
            }
        }
        await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a new <see cref="NpgsqlConnection"/> instance for the provided connection string.
    /// </summary>
    /// <param name="connectionString">Npgsql connection string.</param>
    /// <returns>A new <see cref="NpgsqlConnection"/>.</returns>
    protected virtual NpgsqlConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens the supplied <see cref="NpgsqlConnection"/> synchronously.
    /// </summary>
    /// <param name="connection">Connection to open.</param>
    protected virtual void OpenConnection(NpgsqlConnection connection) => connection.Open();

    /// <summary>
    /// Opens the supplied <see cref="NpgsqlConnection"/> asynchronously.
    /// </summary>
    /// <param name="connection">Connection to open.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <returns>A task that completes when the connection is open.</returns>
    protected virtual Task OpenConnectionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);

    /// <summary>
    /// Begins a new transaction using the default isolation level.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string host, string database, string username, string password)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(host, database, username, password);

            _transactionConnection = new NpgsqlConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction();
        }
    }

    /// <summary>
    /// Begins a new transaction using the specified isolation level.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="isolationLevel">Transaction isolation level.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string host, string database, string username, string password, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(host, database, username, password);

            _transactionConnection = new NpgsqlConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction(isolationLevel);
        }
    }

    /// <summary>
    /// Asynchronously begins a new transaction using the default isolation level.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual async Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }

        var connectionString = BuildConnectionString(host, database, username, password);

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
#else
        var transaction = connection.BeginTransaction();
#endif

        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                transaction.Dispose();
                connection.Dispose();
                throw new DbaTransactionException("Transaction already started.");
            }

            _transactionConnection = connection;
            _transaction = transaction;
        }
    }

    /// <summary>
    /// Asynchronously begins a new transaction using the specified isolation level.
    /// </summary>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="isolationLevel">Transaction isolation level.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual async Task BeginTransactionAsync(string host, string database, string username, string password, IsolationLevel isolationLevel, CancellationToken cancellationToken = default)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }

        var connectionString = BuildConnectionString(host, database, username, password);

        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
#else
        var transaction = connection.BeginTransaction(isolationLevel);
#endif

        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                transaction.Dispose();
                connection.Dispose();
                throw new DbaTransactionException("Transaction already started.");
            }

            _transactionConnection = connection;
            _transaction = transaction;
        }
    }

    /// <summary>
    /// Commits the currently active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual void Commit()
    {
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            _transaction.Commit();
            DisposeTransactionLocked();
        }
    }

    /// <summary>
    /// Asynchronously commits the currently active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
        }
        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.CommitAsync(cancellationToken).ConfigureAwait(false);
#else
            tx!.Commit();
#endif
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
        }
    }

    /// <summary>
    /// Rolls back the currently active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual void Rollback()
    {
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            _transaction.Rollback();
            DisposeTransactionLocked();
        }
    }

    /// <summary>
    /// Asynchronously rolls back the currently active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        NpgsqlTransaction? tx;
        NpgsqlConnection? conn;
        lock (_syncRoot)
        {
            if (_transaction == null)
            {
                throw new DbaTransactionException("No active transaction.");
            }
            tx = _transaction;
            conn = _transactionConnection;
            _transaction = null;
            _transactionConnection = null;
        }
        try
        {
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
            await tx!.RollbackAsync(cancellationToken).ConfigureAwait(false);
#else
            tx!.Rollback();
#endif
        }
        finally
        {
            tx!.Dispose();
            conn?.Dispose();
        }
    }

    private void DisposeTransaction()
    {
        lock (_syncRoot)
        {
            DisposeTransactionLocked();
        }
    }

    private void DisposeTransactionLocked()
    {
        _transaction?.Dispose();
        _transaction = null;
        _transactionConnection?.Dispose();
        _transactionConnection = null;
    }

    /// <inheritdoc />
    protected override bool IsTransient(Exception ex) =>
        ex is PostgresException pgEx &&
        pgEx.SqlState is "40001" or "40P01" or "55P03" or "53300" or "55006";

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }
        base.Dispose(disposing);
    }

    /// <summary>
    /// Executes the provided queries in parallel, optionally throttled by a maximum degree of parallelism.
    /// </summary>
    /// <param name="queries">Collection of SQL queries to execute.</param>
    /// <param name="host">Host name or IP address of the PostgreSQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">User password.</param>
    /// <param name="cancellationToken">Token used to cancel pending operations.</param>
    /// <param name="maxDegreeOfParallelism">Optional throttle limiting concurrent executions.</param>
    /// <returns>A read-only list containing the materialized results for each query.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="queries"/> is <see langword="null"/>.</exception>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string host, string database, string username, string password, CancellationToken cancellationToken = default, int? maxDegreeOfParallelism = null)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        SemaphoreSlim? throttler = null;
        if (maxDegreeOfParallelism.HasValue && maxDegreeOfParallelism.Value > 0)
        {
            throttler = new SemaphoreSlim(maxDegreeOfParallelism.Value);
        }

        var taskList = new List<Task<object?>>();
        foreach (var q in queries)
        {
            if (throttler != null)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            var task = QueryAsync(host, database, username, password, q, null, false, cancellationToken);
            if (throttler != null)
            {
                // Ensure release happens after completion; execute synchronously to reduce overhead
                task = task.ContinueWith(t => { throttler.Release(); return t.Result; }, cancellationToken, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
            taskList.Add(task);
        }

        var results = await Task.WhenAll(taskList).ConfigureAwait(false);
        throttler?.Dispose();
        return results;
    }
}
