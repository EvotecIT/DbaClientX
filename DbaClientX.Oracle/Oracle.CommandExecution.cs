using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Oracle.ManagedDataAccess.Client;

namespace DBAClientX;

public partial class Oracle
{
    private sealed class OracleParameterTypeMap : Dictionary<string, DbType>
    {
        public OracleParameterTypeMap(IDictionary<string, OracleDbType> providerTypes)
            : base(providerTypes.Count, StringComparer.Ordinal)
        {
            ProviderTypes = new Dictionary<string, OracleDbType>(providerTypes, StringComparer.Ordinal);
            foreach (var pair in providerTypes)
            {
                var parameter = new OracleParameter { OracleDbType = pair.Value };
                this[pair.Key] = parameter.DbType;
            }
        }

        public IReadOnlyDictionary<string, OracleDbType> ProviderTypes { get; }
    }

    /// <summary>
    /// Executes a SQL query and materializes the results into the default <see cref="DatabaseClientBase"/> return format.
    /// </summary>
    public virtual object? Query(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Executes a SQL query and returns the first column of the first row in the result set.
    /// </summary>
    public virtual object? ExecuteScalar(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteScalar(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute scalar query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    /// <summary>
    /// Executes a SQL statement that does not return rows, such as INSERT, UPDATE, or DELETE.
    /// </summary>
    public virtual int ExecuteNonQuery(
        string host,
        string serviceName,
        string username,
        string password,
        string query,
        IDictionary<string, object?>? parameters = null,
        bool useTransaction = false,
        IDictionary<string, OracleDbType>? parameterTypes = null,
        IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        var connectionString = BuildConnectionString(host, serviceName, username, password);

        OracleConnection? connection = null;
        OracleTransaction? transaction = null;
        var dispose = false;
        try
        {
            (connection, transaction, dispose) = ResolveConnection(connectionString, useTransaction);
            var dbTypes = ConvertParameterTypes(parameterTypes);
            return ExecuteNonQuery(connection, transaction, query, parameters, dbTypes, parameterDirections);
        }
        catch (Exception ex)
        {
            throw new DbaQueryExecutionException("Failed to execute non-query.", query, ex);
        }
        finally
        {
            if (dispose)
            {
                DisposeConnection(connection!);
            }
        }
    }

    private (OracleConnection Connection, OracleTransaction? Transaction, bool Dispose) ResolveConnection(string connectionString, bool useTransaction)
    {
        if (useTransaction)
        {
            lock (_syncRoot)
            {
                if (_transaction == null || _transactionConnection == null)
                {
                    throw new DbaTransactionException("Transaction has not been started.");
                }

                var normalizedConnectionString = NormalizeConnectionString(connectionString);
                if (_transactionConnectionString != null && !string.Equals(_transactionConnectionString, normalizedConnectionString, StringComparison.OrdinalIgnoreCase))
                {
                    throw new DbaTransactionException("The requested connection details do not match the active transaction.");
                }

                return (_transactionConnection, _transaction, false);
            }
        }

        var connection = CreateConnection(connectionString);
        try
        {
            OpenConnection(connection);
            return (connection, null, true);
        }
        catch
        {
            DisposeConnection(connection);
            throw;
        }
    }

    /// <inheritdoc />
    protected override void AddParameters(DbCommand command, IDictionary<string, object?>? parameters, IDictionary<string, DbType>? parameterTypes = null, IDictionary<string, ParameterDirection>? parameterDirections = null)
    {
        if (command is not OracleCommand oracleCommand || parameterTypes is not OracleParameterTypeMap oracleTypes)
        {
            base.AddParameters(command, parameters, parameterTypes, parameterDirections);
            return;
        }

        if (parameters == null)
        {
            return;
        }

        foreach (var pair in parameters)
        {
            var value = pair.Value ?? DBNull.Value;
            var parameter = new OracleParameter
            {
                ParameterName = pair.Key,
                Value = value
            };

            if (oracleTypes.ProviderTypes.TryGetValue(pair.Key, out var providerType))
            {
                parameter.OracleDbType = providerType;
            }
            else if (parameterTypes.TryGetValue(pair.Key, out var explicitType))
            {
                parameter.DbType = explicitType;
            }
            else
            {
                parameter.DbType = InferParameterDbType(value);
            }

            if (parameterDirections != null && parameterDirections.TryGetValue(pair.Key, out var direction))
            {
                parameter.Direction = direction;
            }

            oracleCommand.Parameters.Add(parameter);
        }
    }

    internal static IDictionary<string, DbType>? ConvertParameterTypes(IDictionary<string, OracleDbType>? types)
        => types == null ? null : new OracleParameterTypeMap(types);

    private static DbType InferParameterDbType(object? value)
    {
        if (value == null || value == DBNull.Value) return DbType.Object;
        if (value is Guid) return DbType.Guid;
        if (value is byte[]) return DbType.Binary;
        if (value is TimeSpan) return DbType.Time;
        if (value is DateTimeOffset) return DbType.DateTimeOffset;

        return Type.GetTypeCode(value.GetType()) switch
        {
            TypeCode.Byte => DbType.Byte,
            TypeCode.SByte => DbType.SByte,
            TypeCode.Int16 => DbType.Int16,
            TypeCode.Int32 => DbType.Int32,
            TypeCode.Int64 => DbType.Int64,
            TypeCode.UInt16 => DbType.UInt16,
            TypeCode.UInt32 => DbType.UInt32,
            TypeCode.UInt64 => DbType.UInt64,
            TypeCode.Decimal => DbType.Decimal,
            TypeCode.Double => DbType.Double,
            TypeCode.Single => DbType.Single,
            TypeCode.Boolean => DbType.Boolean,
            TypeCode.String => DbType.String,
            TypeCode.Char => DbType.StringFixedLength,
            TypeCode.DateTime => DbType.DateTime,
            _ => DbType.Object
        };
    }
}
