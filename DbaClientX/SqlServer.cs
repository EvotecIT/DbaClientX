using System;
using System.Data;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBAClientX;

/// <summary>
/// This class is used to connect to SQL Server
/// </summary>
public class SqlServer
{
    private readonly object _syncRoot = new();
    private ReturnType _returnType;
    private int _commandTimeout;
    private SqlConnection? _transactionConnection;
    private SqlTransaction? _transaction;

    public bool IsInTransaction => _transaction != null;

    public ReturnType ReturnType
    {
        get { lock (_syncRoot) { return _returnType; } }
        set { lock (_syncRoot) { _returnType = value; } }
    }

    public int CommandTimeout
    {
        get { lock (_syncRoot) { return _commandTimeout; } }
        set { lock (_syncRoot) { _commandTimeout = value; } }
    }

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
        object? result = null;
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

            var command = new SqlCommand(query, connection);
            if (useTransaction)
            {
                command.Transaction = _transaction;
            }
            AddParameters(command, parameters, parameterTypes);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }
            var dataAdapter = new SqlDataAdapter(command);
            var dataSet = new System.Data.DataSet();

            dataAdapter.Fill(dataSet);

            var returnType = ReturnType;
            if (returnType == ReturnType.DataRow || returnType == ReturnType.PSObject)
            {
                if (dataSet.Tables.Count > 0)
                {
                    result = dataSet.Tables[0];
                }
            }
            else if (returnType == ReturnType.DataSet)
            {
                result = dataSet;
            }
            else if (returnType == ReturnType.DataTable)
            {
                result = dataSet.Tables;
            }
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
        object? result = null;
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

            var command = new SqlCommand(query, connection);
            if (useTransaction)
            {
                command.Transaction = _transaction;
            }
            AddParameters(command, parameters, parameterTypes);
            var commandTimeout = CommandTimeout;
            if (commandTimeout > 0)
            {
                command.CommandTimeout = commandTimeout;
            }

            var dataSet = new DataSet();
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            var tableIndex = 0;
            do {
                var dataTable = new DataTable($"Table{tableIndex}");
                dataTable.Load(reader);
                dataSet.Tables.Add(dataTable);
                tableIndex++;
            } while (!reader.IsClosed && await reader.NextResultAsync(cancellationToken).ConfigureAwait(false));

            var returnType = ReturnType;
            if (returnType == ReturnType.DataRow || returnType == ReturnType.PSObject)
            {
                if (dataSet.Tables.Count > 0)
                {
                    result = dataSet.Tables[0];
                }
            }
            else if (returnType == ReturnType.DataSet)
            {
                result = dataSet;
            }
            else if (returnType == ReturnType.DataTable)
            {
                result = dataSet.Tables;
            }
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

        return result;
    }

    protected virtual void AddParameters(SqlCommand command, IDictionary<string, object?>? parameters, IDictionary<string, SqlDbType>? parameterTypes = null)
    {
        if (parameters == null)
        {
            return;
        }

        foreach (var pair in parameters)
        {
            var value = pair.Value ?? DBNull.Value;
            var parameter = command.CreateParameter();
            parameter.ParameterName = pair.Key;
            parameter.Value = value;
            if (parameterTypes != null && parameterTypes.TryGetValue(pair.Key, out var explicitType))
            {
                parameter.SqlDbType = explicitType;
            }
            else
            {
                parameter.SqlDbType = InferSqlDbType(value);
            }
            command.Parameters.Add(parameter);
        }
    }

    private static SqlDbType InferSqlDbType(object? value)
    {
        if (value == null || value == DBNull.Value) return SqlDbType.Variant;
        return Type.GetTypeCode(value.GetType()) switch
        {
            TypeCode.Byte => SqlDbType.TinyInt,
            TypeCode.Int16 => SqlDbType.SmallInt,
            TypeCode.Int32 => SqlDbType.Int,
            TypeCode.Int64 => SqlDbType.BigInt,
            TypeCode.Decimal => SqlDbType.Decimal,
            TypeCode.Double => SqlDbType.Float,
            TypeCode.Single => SqlDbType.Real,
            TypeCode.Boolean => SqlDbType.Bit,
            TypeCode.String => SqlDbType.NVarChar,
            _ => SqlDbType.Variant
        };
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