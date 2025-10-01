using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using MySqlConnector;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with a MySQL database using the shared <see cref="DatabaseClientBase"/> abstractions.
/// </summary>
public class MySql : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private MySqlConnection? _transactionConnection;
    private MySqlTransaction? _transaction;

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
    /// Builds a <see cref="MySqlConnectionStringBuilder"/> connection string from individual connection components.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="port">Optional TCP port; when omitted the provider default is used.</param>
    /// <param name="ssl">Optional SSL requirement flag; <see langword="true"/> enforces TLS.</param>
    /// <returns>The generated connection string.</returns>
    /// <remarks>
    /// The builder enables connection pooling by default so repeated operations reuse the same socket where possible.
    /// </remarks>
    public static string BuildConnectionString(string host, string database, string username, string password, uint? port = null, bool? ssl = null)
    {
        var builder = new MySqlConnectionStringBuilder
        {
            Server = host,
            Database = database,
            UserID = username,
            Password = password,
            Pooling = true
        };
        if (port.HasValue)
        {
            builder.Port = port.Value;
        }
        if (ssl.HasValue)
        {
            builder.SslMode = ssl.Value ? MySqlSslMode.Required : MySqlSslMode.None;
        }
        return builder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified MySQL instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Exceptions are swallowed to keep the call lightweight; call
    /// <see cref="ExecuteScalar(string, string, string, string, string, IDictionary{string, object}, bool, IDictionary{string, MySqlDbType}, IDictionary{string, ParameterDirection})" /> for detailed error information.
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
    /// Performs an asynchronous connectivity test against the specified MySQL instance.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
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
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The materialized query result; the concrete shape matches the <c>DatabaseClientBase</c> contract.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? Query(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a SQL query and returns the first column of the first row in the result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The scalar value produced by the query, or <see langword="null"/> when no rows are returned.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteScalar(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, MySqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new MySqlParameter(), static (p, t) => p.MySqlDbType = t);

    /// <summary>
    /// Executes a SQL statement that does not produce a result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
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
        IDictionary<string, MySqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a non-query SQL statement asynchronously.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The number of affected rows.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<int> ExecuteNonQueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a SQL query asynchronously and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The materialized query result.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> QueryAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a SQL query asynchronously and returns the first column of the first row in the result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The scalar value produced by the query, or <see langword="null"/> when no rows are returned.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteScalarAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a stored procedure and returns the aggregated results.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The materialized result as defined by the base client.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a stored procedure asynchronously and returns the aggregated results.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>The materialized result as defined by the base client.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a stored procedure using explicitly configured <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Sequence of parameters to add directly to the command.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <returns>The materialized result as defined by the base client.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteStoredProcedure(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Executes a stored procedure asynchronously using explicitly configured <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Sequence of parameters to add directly to the command.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <returns>The materialized result as defined by the base client.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default)
    {
        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Streams rows produced by a query asynchronously without buffering the entire result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the streaming operation.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>An <see cref="IAsyncEnumerable{DataRow}"/> that yields <see cref="DataRow"/> instances as they arrive from the server.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(string host, string database, string username, string password, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Streams rows produced by a stored procedure asynchronously without buffering the entire result set.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the streaming operation.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="MySqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <returns>An <see cref="IAsyncEnumerable{DataRow}"/> that yields <see cref="DataRow"/> instances as they arrive from the server.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string database, string username, string password, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, MySqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Streams rows produced by a stored procedure using explicitly constructed <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="procedure">Stored procedure name.</param>
    /// <param name="parameters">Sequence of preconfigured parameters to add to the command.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the streaming operation.</param>
    /// <returns>An <see cref="IAsyncEnumerable{DataRow}"/> that yields <see cref="DataRow"/> instances as they arrive from the server.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string host, string database, string username, string password, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(host, database, username, password);

            MySqlConnection? connection = null;
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
                connection = new MySqlConnection(connectionString);
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
    /// Performs a bulk insert using <see cref="MySqlBulkCopy"/> and the provided <see cref="DataTable"/> payload.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="table">Table containing rows to be inserted.</param>
    /// <param name="destinationTable">Target table name in the database.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="batchSize">Optional batch size; enables chunked ingestion for large payloads.</param>
    /// <param name="bulkCopyTimeout">Optional timeout applied to the bulk copy operation.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when <see cref="MySqlBulkCopy"/> reports an error.</exception>
    public virtual void BulkInsert(string host, string database, string username, string password, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
            var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(column.Ordinal, column.ColumnName, null));
            }

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
                    WriteToServer(bulkCopy, batchTable);
                }
            }
            else
            {
                WriteToServer(bulkCopy, table);
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
    /// Performs a bulk insert asynchronously using <see cref="MySqlBulkCopy"/> and the provided <see cref="DataTable"/> payload.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="table">Table containing rows to be inserted.</param>
    /// <param name="destinationTable">Target table name in the database.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call uses the currently open transaction.</param>
    /// <param name="batchSize">Optional batch size; enables chunked ingestion for large payloads.</param>
    /// <param name="bulkCopyTimeout">Optional timeout applied to the bulk copy operation.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when <see cref="MySqlBulkCopy"/> reports an error.</exception>
    public virtual async Task BulkInsertAsync(string host, string database, string username, string password, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null, CancellationToken cancellationToken = default)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(host, database, username, password);

        MySqlConnection? connection = null;
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
            var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
            bulkCopy.DestinationTableName = destinationTable;
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(new MySqlBulkCopyColumnMapping(column.Ordinal, column.ColumnName, null));
            }

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
                    await WriteToServerAsync(bulkCopy, batchTable, cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
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
    /// Creates a configured <see cref="MySqlBulkCopy"/> instance for bulk operations.
    /// </summary>
    /// <param name="connection">Open MySQL connection.</param>
    /// <param name="transaction">Optional ambient transaction.</param>
    /// <returns>A <see cref="MySqlBulkCopy"/> bound to the provided connection.</returns>
    protected virtual MySqlBulkCopy CreateBulkCopy(MySqlConnection connection, MySqlTransaction? transaction) => new(connection, transaction);

    /// <summary>
    /// Writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual void WriteToServer(MySqlBulkCopy bulkCopy, DataTable table) => bulkCopy.WriteToServer(table);

    /// <summary>
    /// Asynchronously writes the contents of <paramref name="table"/> to the server using the provided bulk copy instance.
    /// </summary>
    protected virtual Task WriteToServerAsync(MySqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken) => bulkCopy.WriteToServerAsync(table, cancellationToken).AsTask();

    /// <summary>
    /// Creates a new <see cref="MySqlConnection"/> for the supplied connection string.
    /// </summary>
    protected virtual MySqlConnection CreateConnection(string connectionString) => new(connectionString);

    /// <summary>
    /// Opens a MySQL connection using synchronous APIs.
    /// </summary>
    protected virtual void OpenConnection(MySqlConnection connection) => connection.Open();

    /// <summary>
    /// Opens a MySQL connection asynchronously.
    /// </summary>
    protected virtual Task OpenConnectionAsync(MySqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);
    /// <summary>
    /// Starts a transaction using the default isolation level (<see cref="IsolationLevel.ReadCommitted"/>).
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already in progress.</exception>
    public virtual void BeginTransaction(string host, string database, string username, string password)
        => BeginTransaction(host, database, username, password, IsolationLevel.ReadCommitted);

    /// <summary>
    /// Starts a transaction with the specified <paramref name="isolationLevel"/>.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="isolationLevel">Desired transaction isolation level.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already in progress.</exception>
    public virtual void BeginTransaction(string host, string database, string username, string password, IsolationLevel isolationLevel)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(host, database, username, password);

            _transactionConnection = new MySqlConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction(isolationLevel);
        }
    }

    /// <summary>
    /// Asynchronously starts a transaction using the default isolation level (<see cref="IsolationLevel.ReadCommitted"/>).
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="cancellationToken">Token used to cancel connection establishment.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already in progress.</exception>
    public virtual Task BeginTransactionAsync(string host, string database, string username, string password, CancellationToken cancellationToken = default)
        => BeginTransactionAsync(host, database, username, password, IsolationLevel.ReadCommitted, cancellationToken);

    /// <summary>
    /// Asynchronously starts a transaction with the specified <paramref name="isolationLevel"/>.
    /// </summary>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="isolationLevel">Desired transaction isolation level.</param>
    /// <param name="cancellationToken">Token used to cancel connection establishment.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already in progress.</exception>
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

        var connection = new MySqlConnection(connectionString);
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
    /// Commits the active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no active transaction exists.</exception>
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
    /// Asynchronously commits the active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the commit operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when no active transaction exists.</exception>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }

        await _transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        DisposeTransaction();
    }

    /// <summary>
    /// Rolls back the active transaction.
    /// </summary>
    /// <exception cref="DbaTransactionException">Thrown when no active transaction exists.</exception>
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
    /// Asynchronously rolls back the active transaction.
    /// </summary>
    /// <param name="cancellationToken">Token used to cancel the rollback operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when no active transaction exists.</exception>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }

        await _transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);
        DisposeTransaction();
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
        ex is MySqlException mysqlEx &&
        mysqlEx.ErrorCode is MySqlErrorCode.ConnectionCountError
            or MySqlErrorCode.LockDeadlock
            or MySqlErrorCode.LockWaitTimeout
            or MySqlErrorCode.UnableToConnectToHost
            or MySqlErrorCode.XARBDeadlock;

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
    /// Executes multiple queries concurrently against the same connection information.
    /// </summary>
    /// <param name="queries">Collection of SQL statements to execute.</param>
    /// <param name="host">Host name or IP address of the MySQL server.</param>
    /// <param name="database">Database (schema) to connect to.</param>
    /// <param name="username">User identifier.</param>
    /// <param name="password">Password for the provided <paramref name="username"/>.</param>
    /// <param name="cancellationToken">Token used to cancel the overall batch.</param>
    /// <param name="maxDegreeOfParallelism">Optional concurrency limiter; <see langword="null"/> uses as many tasks as queries.</param>
    /// <returns>Results returned by each query in submission order.</returns>
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

        var tasks = queries.Select(async q =>
        {
            if (throttler != null)
            {
                await throttler.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            try
            {
                return await QueryAsync(host, database, username, password, q, null, false, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                throttler?.Release();
            }
        });

        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        throttler?.Dispose();
        return results;
    }
}
