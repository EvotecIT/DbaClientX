using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

/// <summary>
/// This class is used to connect to SQL Server
/// </summary>
public class SqlServer : DatabaseClientBase
{
    private readonly object _syncRoot = new();
    private SqlConnection? _transactionConnection;
    private SqlTransaction? _transaction;

    public bool IsInTransaction => _transaction != null;

    public virtual object? SqlQuery(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, IDictionary<string, SqlDbType>? parameterTypes = null)
    {
        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        }.ConnectionString;

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
            return ExecuteQuery(connection, useTransaction ? _transaction : null, query, parameters, dbTypes);
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

    private static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, SqlDbType>? types)
    {
        if (types == null)
        {
            return null;
        }

        var result = new Dictionary<string, DbType>(types.Count);
        foreach (var pair in types)
        {
            var parameter = new SqlParameter { SqlDbType = pair.Value };
            result[pair.Key] = parameter.DbType;
        }
        return result;
    }

    public virtual async Task<object?> SqlQueryAsync(string serverOrInstance, string database, bool integratedSecurity, string query, IDictionary<string, object?>? parameters = null, bool useTransaction = false, CancellationToken cancellationToken = default, IDictionary<string, SqlDbType>? parameterTypes = null)
    {
        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        }.ConnectionString;

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
            return await ExecuteQueryAsync(connection, useTransaction ? _transaction : null, query, parameters, cancellationToken, dbTypes).ConfigureAwait(false);
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


    public virtual void BeginTransaction(string serverOrInstance, string database, bool integratedSecurity)
    {
        if (_transaction != null)
        {
            throw new DbaTransactionException("Transaction already started.");
        }

        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = serverOrInstance,
            InitialCatalog = database,
            IntegratedSecurity = integratedSecurity,
            Pooling = true
        }.ConnectionString;

        _transactionConnection = new SqlConnection(connectionString);
        _transactionConnection.Open();
        _transaction = _transactionConnection.BeginTransaction();
    }

    public virtual void Commit()
    {
        if (_transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }
        _transaction.Commit();
        DisposeTransaction();
    }

    public virtual void Rollback()
    {
        if (_transaction == null)
        {
            throw new DbaTransactionException("No active transaction.");
        }
        _transaction.Rollback();
        DisposeTransaction();
    }

    private void DisposeTransaction()
    {
        _transaction?.Dispose();
        _transaction = null;
        _transactionConnection?.Dispose();
        _transactionConnection = null;
    }

    public async Task<IReadOnlyList<object?>> RunQueriesInParallel(IEnumerable<string> queries, string serverOrInstance, string database, bool integratedSecurity, CancellationToken cancellationToken = default)
    {
        if (queries == null)
        {
            throw new ArgumentNullException(nameof(queries));
        }

        var tasks = queries.Select(q => SqlQueryAsync(serverOrInstance, database, integratedSecurity, q, null, false, cancellationToken));
        var results = await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }
}