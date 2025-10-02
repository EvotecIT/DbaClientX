using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER
using System.Runtime.CompilerServices;
#endif

namespace DBAClientX;

/// <summary>
/// Provides high-level convenience operations for interacting with Microsoft SQL Server using the shared
/// <see cref="DatabaseClientBase"/> infrastructure.
/// </summary>
/// <remarks>
/// The implementation mirrors the patterns exposed by <see cref="MySql"/> to ensure a predictable experience when
/// switching between providers. All database access is funneled through the base class helpers so parameter handling,
/// exception wrapping, and result projection behave consistently across engines.
/// </remarks>
public class SqlServer : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private SqlConnection? _transactionConnection;
    private SqlTransaction? _transaction;

    /// <summary>
    /// Gets a value indicating whether a transaction scope is currently active for the client.
    /// </summary>
    /// <remarks>
    /// The property becomes <see langword="true"/> when any <c>BeginTransaction</c> overload succeeds and returns to
    /// <see langword="false"/> after <see cref="Commit"/>, <see cref="Rollback"/>, or their asynchronous counterparts
    /// complete. Consumers can poll this property to make idempotent decisions about transaction flow.
    /// </remarks>
    public bool IsInTransaction => _transaction != null;

    /// <summary>
    /// Builds a SQL Server connection string from discrete connection components.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="port">Optional TCP port appended to <paramref name="serverOrInstance"/>.</param>
    /// <param name="ssl">Optional encryption flag routed to <see cref="SqlConnectionStringBuilder.Encrypt"/>.</param>
    /// <returns>A provider-formatted connection string.</returns>
    /// <remarks>
    /// The builder enables pooling by default to keep performance comparable to the MySQL implementation. Additional
    /// advanced options can be appended by callers if necessary for their environment.
    /// </remarks>
    public static string BuildConnectionString(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null, int? port = null, bool? ssl = null)
    {
        var dataSource = port.HasValue ? $"{serverOrInstance},{port.Value}" : serverOrInstance;
        var connectionStringBuilder = new SqlConnectionStringBuilder
        {
            DataSource = dataSource,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        };
        if (!integratedSecurity)
        {
            connectionStringBuilder.UserID = username;
            connectionStringBuilder.Password = password;
        }
        if (ssl.HasValue)
        {
            connectionStringBuilder.Encrypt = ssl.Value;
        }
        return connectionStringBuilder.ConnectionString;
    }

    /// <summary>
    /// Performs a synchronous connectivity test against the specified SQL Server instance.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Errors are intentionally swallowed to deliver a lightweight health probe. Use <see cref="ExecuteScalar(string, string, bool, string, IDictionary{string, object?}?, bool, IDictionary{string, SqlDbType}?, IDictionary{string, ParameterDirection}?, string?, string?)"/>
    /// when detailed exception information is required.
    /// </remarks>
    public virtual bool Ping(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
    {
        try
        {
            ExecuteScalar(serverOrInstance, database, integratedSecurity, "SELECT 1", username: username, password: password);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Asynchronously performs a connectivity test against the specified SQL Server instance.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying query.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns><see langword="true"/> when executing <c>SELECT 1</c> succeeds; otherwise <see langword="false"/>.</returns>
    /// <remarks>
    /// Mirrors <see cref="Ping"/> but leverages async I/O primitives to avoid blocking threads in scalable environments.
    /// </remarks>
    public virtual async Task<bool> PingAsync(string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        try
        {
            await ExecuteScalarAsync(serverOrInstance, database, integratedSecurity, "SELECT 1", cancellationToken: cancellationToken, username: username, password: password).ConfigureAwait(false);
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
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The materialized query result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <remarks>
    /// Always prefer parameterized SQL by supplying <paramref name="parameters"/> (and optionally
    /// <paramref name="parameterTypes"/>) to guard against SQL injection attacks. Avoid concatenating user input into
    /// <paramref name="query"/> directly.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? Query(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Executes a SQL query that returns a single scalar value.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The first column of the first row returned by the query, or <see langword="null"/> when no data is produced.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteScalar(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqlDbType>? types) =>
        DbTypeConverter.ConvertParameterTypes(types, static () => new SqlParameter(), static (p, t) => p.SqlDbType = t);

    /// <summary>
    /// Executes a SQL statement that does not return a result set (for example, <c>INSERT</c>, <c>UPDATE</c>, or <c>DELETE</c>).
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The number of rows affected as reported by the provider.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual int ExecuteNonQuery(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a SQL statement that does not return a result set.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The number of rows affected as reported by the provider.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<int> ExecuteNonQueryAsync(
        string serverOrInstance,
        string database,
        bool integratedSecurity,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        CancellationToken cancellationToken = default,
        IDictionary<string, SqlDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null,
        string? username = null,
        string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a SQL query and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The materialized query result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> QueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a SQL query that returns a single scalar value.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The first column of the first row returned by the query, or <see langword="null"/> when no data is produced.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteScalarAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Executes a stored procedure and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="procedure">Stored procedure name, optionally schema-qualified.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The materialized procedure result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteStoredProcedure(string serverOrInstance, string database, bool integratedSecurity, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a stored procedure and materializes the result using the shared <see cref="DatabaseClientBase"/> pipeline.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="procedure">Stored procedure name, optionally schema-qualified.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The materialized procedure result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string serverOrInstance, string database, bool integratedSecurity, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Executes a stored procedure using an existing collection of <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="procedure">Stored procedure name, optionally schema-qualified.</param>
    /// <param name="parameters">Optional sequence of preconfigured parameters.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The materialized procedure result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual object? ExecuteStoredProcedure(string serverOrInstance, string database, bool integratedSecurity, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a stored procedure using an existing collection of <see cref="DbParameter"/> instances.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="procedure">Stored procedure name, optionally schema-qualified.</param>
    /// <param name="parameters">Optional sequence of preconfigured parameters.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the underlying command.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>The materialized procedure result as defined by the <see cref="DatabaseClientBase"/> implementation.</returns>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual async Task<object?> ExecuteStoredProcedureAsync(string serverOrInstance, string database, bool integratedSecurity, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a SQL query and streams the resulting rows.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="query">SQL text to execute.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the enumeration.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>An async stream of <see cref="DataRow"/> instances.</returns>
    /// <remarks>
    /// Streaming mirrors the behaviour of the MySQL provider to minimise cross-engine surprises. Ensure the stream is
    /// fully enumerated or disposed to release underlying resources promptly.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual IAsyncEnumerable<DataRow> QueryStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a stored procedure and streams the resulting rows.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="procedure">Stored procedure name, optionally schema-qualified.</param>
    /// <param name="parameters">Optional set of parameter name/value pairs.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the enumeration.</param>
    /// <param name="parameterTypes">Optional map of parameter types expressed as <see cref="SqlDbType"/> values.</param>
    /// <param name="parameterDirections">Optional map of parameter directions.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>An async stream of <see cref="DataRow"/> instances.</returns>
    /// <remarks>
    /// Streaming stored procedures is useful for large result sets where buffering entire tables would be prohibitive.
    /// Always enumerate the stream or dispose the enumerator to release provider resources promptly.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string procedure, IDictionary<string, object?>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null, string? username = null, string? password = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Asynchronously executes a stored procedure using <see cref="DbParameter"/> instances and streams the resulting rows.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="procedure">Stored procedure name, optionally schema-qualified.</param>
    /// <param name="parameters">Optional sequence of preconfigured parameters.</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the enumeration.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <returns>An async stream of <see cref="DataRow"/> instances.</returns>
    /// <remarks>
    /// Use this overload when working with higher-level APIs that already expose <see cref="DbParameter"/> objects. As
    /// with other streaming methods, fully consume the iterator to dispose resources deterministically.
    /// </remarks>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying command fails.</exception>
    public virtual IAsyncEnumerable<DataRow> ExecuteStoredProcedureStreamAsync(string serverOrInstance, string database, bool integratedSecurity, string procedure, IEnumerable<DbParameter>? parameters = null, bool useTransaction = false, [EnumeratorCancellation] CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        return Stream();

        async IAsyncEnumerable<DataRow> Stream()
        {
            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            SqlConnection? connection = null;
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
                connection = new SqlConnection(connectionString);
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
    /// Uses <see cref="SqlBulkCopy"/> to insert data contained in <paramref name="table"/> into the specified destination table.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="table">Source data table.</param>
    /// <param name="destinationTable">Target table name (optionally schema-qualified).</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="batchSize">Optional batch size hint for <see cref="SqlBulkCopy"/>.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (seconds) for the bulk copy operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <remarks>
    /// Column mappings are generated automatically by matching column names. Ensure that the source table columns align with the destination schema to avoid runtime errors.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying bulk copy fails.</exception>
    public virtual void BulkInsert(string serverOrInstance, string database, bool integratedSecurity, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null, string? username = null, string? password = null)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
            using var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
            bulkCopy.DestinationTableName = destinationTable;
            if (batchSize.HasValue)
            {
                bulkCopy.BatchSize = batchSize.Value;
            }
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            WriteToServer(bulkCopy, table);
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
    /// Asynchronously uses <see cref="SqlBulkCopy"/> to insert data contained in <paramref name="table"/> into the specified destination table.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="table">Source data table.</param>
    /// <param name="destinationTable">Target table name (optionally schema-qualified).</param>
    /// <param name="useTransaction">When <see langword="true"/> the call reuses the currently open transaction.</param>
    /// <param name="batchSize">Optional batch size hint for <see cref="SqlBulkCopy"/>.</param>
    /// <param name="bulkCopyTimeout">Optional timeout (seconds) for the bulk copy operation.</param>
    /// <param name="cancellationToken">Token used to cancel the bulk copy operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <remarks>
    /// Column mappings are generated automatically by matching column names. Ensure that the source table columns align with the destination schema to avoid runtime errors.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="DbaTransactionException">Thrown when <paramref name="useTransaction"/> is <see langword="true"/> but no transaction has been started.</exception>
    /// <exception cref="DbaQueryExecutionException">Thrown when the underlying bulk copy fails.</exception>
    public virtual async Task BulkInsertAsync(string serverOrInstance, string database, bool integratedSecurity, DataTable table, string destinationTable, bool useTransaction = false, int? batchSize = null, int? bulkCopyTimeout = null, CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        if (table == null) throw new ArgumentNullException(nameof(table));

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        SqlConnection? connection = null;
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
            using var bulkCopy = CreateBulkCopy(connection, useTransaction ? _transaction : null);
            bulkCopy.DestinationTableName = destinationTable;
            if (batchSize.HasValue)
            {
                bulkCopy.BatchSize = batchSize.Value;
            }
            if (bulkCopyTimeout.HasValue)
            {
                bulkCopy.BulkCopyTimeout = bulkCopyTimeout.Value;
            }

            foreach (DataColumn column in table.Columns)
            {
                bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
            }

            await WriteToServerAsync(bulkCopy, table, cancellationToken).ConfigureAwait(false);
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

    protected virtual SqlBulkCopy CreateBulkCopy(SqlConnection connection, SqlTransaction? transaction) => new(connection, SqlBulkCopyOptions.Default, transaction);

    protected virtual void WriteToServer(SqlBulkCopy bulkCopy, DataTable table) => bulkCopy.WriteToServer(table);

    protected virtual Task WriteToServerAsync(SqlBulkCopy bulkCopy, DataTable table, CancellationToken cancellationToken) => bulkCopy.WriteToServerAsync(table, cancellationToken);

    protected virtual SqlConnection CreateConnection(string connectionString) => new(connectionString);

    protected virtual void OpenConnection(SqlConnection connection) => connection.Open();

    protected virtual Task OpenConnectionAsync(SqlConnection connection, CancellationToken cancellationToken) => connection.OpenAsync(cancellationToken);


    /// <summary>
    /// Begins a transaction using the default isolation level (<see cref="IsolationLevel.ReadCommitted"/>).
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, string? username = null, string? password = null)
        => BeginTransaction(serverOrInstance, database, integratedSecurity, IsolationLevel.ReadCommitted, username, password);

    /// <summary>
    /// Begins a transaction using the specified isolation level.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="isolationLevel">Isolation level to apply to the transaction.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity, IsolationLevel isolationLevel, string? username = null, string? password = null)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }

            var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

            _transactionConnection = new SqlConnection(connectionString);
            _transactionConnection.Open();
            _transaction = _transactionConnection.BeginTransaction(isolationLevel);
        }
    }

    /// <summary>
    /// Begins a transaction asynchronously using the default isolation level.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual Task BeginTransactionAsync(string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null)
        => BeginTransactionAsync(serverOrInstance, database, integratedSecurity, IsolationLevel.ReadCommitted, cancellationToken, username, password);

    /// <summary>
    /// Begins a transaction asynchronously using the specified isolation level.
    /// </summary>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="isolationLevel">Isolation level to apply to the transaction.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <exception cref="DbaTransactionException">Thrown when a transaction is already active.</exception>
    public virtual async Task BeginTransactionAsync(string serverOrInstance, string database, bool integratedSecurity, IsolationLevel isolationLevel, CancellationToken cancellationToken = default, string? username = null, string? password = null)
    {
        lock (_syncRoot)
        {
            if (_transaction != null)
            {
                throw new DbaTransactionException("Transaction already started.");
            }
        }

        var connectionString = BuildConnectionString(serverOrInstance, database, integratedSecurity, username, password);

        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
#if NETSTANDARD2_1_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NET5_0_OR_GREATER
        var transaction = (SqlTransaction)await connection.BeginTransactionAsync(isolationLevel, cancellationToken).ConfigureAwait(false);
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
    /// <param name="cancellationToken">Token used to cancel the commit operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        SqlTransaction? tx;
        SqlConnection? conn;
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
    /// <param name="cancellationToken">Token used to cancel the rollback operation.</param>
    /// <exception cref="DbaTransactionException">Thrown when no transaction is active.</exception>
    public virtual async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        SqlTransaction? tx;
        SqlConnection? conn;
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

    protected override bool IsTransient(Exception ex) =>
        ex is SqlException sqlEx &&
        sqlEx.Number is 4060 or 10928 or 10929 or 1205 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920;

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeTransaction();
        }
        base.Dispose(disposing);
    }

    /// <summary>
    /// Executes multiple queries concurrently, optionally throttling the degree of parallelism.
    /// </summary>
    /// <param name="queries">Collection of SQL statements to execute.</param>
    /// <param name="serverOrInstance">Server name, address, or <c>Server\Instance</c> style identifier.</param>
    /// <param name="database">Database (catalog) to target.</param>
    /// <param name="integratedSecurity">When <see langword="true"/> configures Windows authentication.</param>
    /// <param name="cancellationToken">Token used to cancel the operation.</param>
    /// <param name="username">SQL login identifier when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="password">SQL login password when <paramref name="integratedSecurity"/> is <see langword="false"/>.</param>
    /// <param name="maxDegreeOfParallelism">Optional limit on the number of concurrent executions.</param>
    /// <returns>A list containing the result of each query in submission order.</returns>
    /// <remarks>
    /// Each query is executed via <see cref="QueryAsync(string, string, bool, string, IDictionary{string, object?}?, bool, CancellationToken, IDictionary{string, SqlDbType}?, IDictionary{string, ParameterDirection}?, string?, string?)"/>.
    /// The method mirrors the MySQL provider behaviour so higher-level orchestration can remain provider agnostic.
    /// </remarks>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="queries"/> is <see langword="null"/>.</exception>
    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default, string? username = null, string? password = null, int? maxDegreeOfParallelism = null)
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
                return await QueryAsync(serverOrInstance, database, integratedSecurity, q, null, false, cancellationToken, username: username, password: password).ConfigureAwait(false);
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
